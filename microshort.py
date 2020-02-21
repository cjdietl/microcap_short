from Execution.ImmediateExecutionModel import ImmediateExecutionModel
from datetime import time
from collections import defaultdict
import numpy as np
from io import StringIO
import pandas as pd
import json
import talib

class MicroShort(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2019, 2, 1)  # Set Start Date
        self.SetEndDate(2019, 2, 10)  # Set Start Date
        
        self.SetCash(30000)  # Set Strategy Cash
        self.SetExecution(ImmediateExecutionModel())
        
        self.SetBrokerageModel(BrokerageName.InteractiveBrokersBrokerage)
        
        self.UniverseSettings.Resolution = Resolution.Minute
        
        self.UniverseSettings.ExtendedMarketHours = True
        
        self.AddEquity("SPY")
        
        self.AddUniverseSelection(ScheduledUniverseSelectionModel(self.DateRules.EveryDay("SPY"),self.TimeRules.At(9, 23),self.SelectSymbols))
    
        self.Schedule.On(self.DateRules.EveryDay("SPY"), self.TimeRules.At(12, 0), self.CoverShorts)
        self.Schedule.On(self.DateRules.EveryDay("SPY"), self.TimeRules.At(9, 30, 30), self.CancelOrders)
        self.Schedule.On(self.DateRules.EveryDay("SPY"), self.TimeRules.At(9, 25), self.FireOrders)
        
        if self.LiveMode:
            self.Schedule.On(self.DateRules.EveryDay("SPY"), self.TimeRules.At(9, 20),self.GetShortList)
        
        self.Schedule.On(self.DateRules.Every(DayOfWeek.Monday),self.TimeRules.At(9, 35),self.RebalanceSPY)
        
        self.lastDay = -1
        
        self.openOrders = []
        self.stopOrders = []
        
        # Keep a record of Ticker to Symbol Mapping
        self.SymbolMap = defaultdict()
        
        self.numberOfGappers = 50
        
        self.numberOfStocks = 5
        
        self.gappers = pd.DataFrame()
        
    def SelectSymbols(self, date):

        self.Log('**** Algorithm Time {} ****'.format(str(self.Time)))
        
        self.Log('**** START Downloading FINVIZ Change List ****')
        
        symbols = [Symbol.Create('SPY', SecurityType.Equity, Market.USA)]

        for i in range(0,5):
            try:
                # fetch the file from dropbox
                dstr = self.Download("https://www.dropbox.com/s/jco6fqx67sczyn6/finviz.csv?dl=1")
                                
                c = StringIO(dstr)
                df = pd.read_csv(c,delimiter=',',comment='#')
                break
            except pd.errors.EmptyDataError:
                self.Log('Could not read FinVIZ Stock list, attempt {}'.format(i))
                return symbols

        self.gappers = df[['Ticker','Price','Change','Volume','Country']]
        
        self.gappers = self.gappers.loc[(self.gappers['Price'] > 0.1) & (self.gappers['Price'] <= 5)]
        
        gappersl = self.gappers['Ticker'].tolist()
        
        self.Log('**** FINVIZ Change List Added****')
    
        for i in range(0,5):
            try:
                # fetch the file from dropbox
                dstr = self.Download("") # Insert here your premarket activity scrape from FinVIZ

                js = json.loads(dstr)
                rows =js['data']['STOCKS']['MostAdvanced']['table']['rows']
                
                nasdaq = pd.DataFrame(rows)
                nasdaq['lastSalePrice'] = nasdaq['lastSalePrice'].str.replace('$', '')
                nasdaq['lastSalePrice']=nasdaq['lastSalePrice'].astype(float)
                
                nasdaq = nasdaq.loc[(nasdaq['lastSalePrice'] > 0.1) & (nasdaq['lastSalePrice'] <= 5)]
                
                self.Log('**** START NASDAQ Pre-Market Activity List ****')
                self.Log(nasdaq)
                self.Log('**** END NASDAQ Pre-Market Activity List ****')
                
                break
            except pd.errors.EmptyDataError:
                self.Log('Could not read NASDAQ Pre-Market Activity List, attempt {}'.format(i))
        
        gappersl = self.gappers['Ticker'].tolist() + nasdaq['symbol'].tolist()
        
        self.Log('NASDAQ Pre-Market and FinVIZ found {}'.format(gappersl))

        self.SymbolMap = dict()
        
        self.Log('**** START adding Stock to Universe ****')
        for s in gappersl:
            newsym = Symbol.Create(s, SecurityType.Equity, Market.USA)
            symbols.append(newsym)
            self.SymbolMap[newsym] = s
        self.Log('**** END adding Stock to Universe ****')
        
        return symbols
    
    def OnData(self, data):
        pass
    
    def RebalanceSPY(self):
        self.Log('Rebalancing SPY')
        self.SetHoldings('SPY', 0.4)

    def GetShortList(self):
        
        self.Log('Start getting shortable stocks list from IB ...')
        
        for i in range(0,5):
            try:
                shortlist = self.Download("") # The list cannot be downloaded directly at IB, but we need to go through Dropbox
                c = StringIO(shortlist)
                df = pd.read_csv(c,delimiter='|',header=1)
                break
            except pd.errors.EmptyDataError:
                self.Log('Could not read IB Shortlist, attempt {}'.format(i))
            
        df = df.drop(columns=['Unnamed: 8'])
        df = df.rename(index=str, columns={"#SYM": "Symbol"})
        
        self.slist = df
        
        self.Log('Finished getting shortable stocks list from IB')
        
    def FindIBSymbol(self,x):
        
        try:
            s = Symbol.Create(x, SecurityType.Equity, Market.USA)
        except ArgumentException:
            #self.Log('Error with Symbol Ticker {}'.format(x))
            return ''
            
        return str(s)
        
    def FireOrders(self):
        
        self.pumpedStocks = {}
        NightChange = dict()
        
        # Gappers = dict()
        
        VolumeGappers = dict()
        
        self.openOrders = []
        self.stopOrders = []
        
        self.Log('Scanning for Gappers ...')
        
        # Find Gappers
        for security in self.ActiveSecurities.Values:
            
            if 'SPY' in str(security.Symbol):
                continue
            
            if security.HasData:
                    closehist = self.History(security.Symbol, 2, Resolution.Daily)
                            
                    if str(security.Symbol) not in closehist.index or closehist.empty:
                        self.Log('{} not found in history data frame.'.format(str(security.Symbol)))
                        continue
            
                    closebars = np.asarray(closehist['close'])
                    
                    if len(closebars) < 2:
                        self.Log('Not enough ticks of {} to compare daily price for gap scan'.format(str(security.Symbol)))
                        continue
                    
                    todayopen = security.Open
                    
                    if self.LiveMode:
                        ref_idx = 1
                    else:
                        ref_idx = 0
                        
                    closeyest = closebars[ref_idx]
                    gap = todayopen/ closeyest - 1

                    sigmahist = self.History(security.Symbol, 100, Resolution.Daily)
                    sigmahist_pct = sigmahist.pct_change()
                    
                    try:
                         sigmahist_close = np.asarray(sigmahist['close'])
                         sigmahist_pct_close = np.asarray(sigmahist_pct['close'])
                    except KeyError:
                         continue
                     
                    ema = talib.EMA(sigmahist_close,timeperiod=20)[-1]
                    
                    if todayopen < ema:
                        continue
                    
                    sigmahist_pct_close = sigmahist_pct_close - np.nanmean(sigmahist_pct_close)
                    sigma = np.nanstd(sigmahist_pct_close)
                    
                    if gap/sigma < 2:
                        continue
                    
                    NightChange[security.Symbol] = gap
        
        Gappers = dict(sorted(NightChange.items(), key = lambda kv: (-round(kv[1], 6), kv[0]))[:self.numberOfGappers])

        self.Log('Scanning for Volume...')
        
        # Rank after Pre-Market DollarVolume
        for s,g in Gappers.items():
            vol_hist = self.History(s, TimeSpan.FromMinutes(30), Resolution.Minute)
            
            if vol_hist.empty:
                continue
            
            #VolumeGappers[s] = spread
            dollarvol = (vol_hist.close*vol_hist.volume).sum()
            VolumeGappers[s] = dollarvol
            
            self.Log('Found {} up {:.2f}%'.format(s,g*100))
        
        self.Log('Issuing Orders ...')
        
        StocksToTrade = dict(sorted(VolumeGappers.items(), key = lambda kv: (-round(kv[1], 6), kv[0])))
        
        if self.LiveMode:
            shorted = 0
            
            for key,value in StocksToTrade.items():
    
                sec = self.ActiveSecurities[key]
                
                #Due to Margin requirements, take only half
                available = self.Portfolio.TotalPortfolioValue*0.5
                
                #Short Sell out of the gates
                shares = round(available/len(StocksToTrade)/sec.Price)
                
                ticker = str(key)
                ticker = ticker.split()[0]
                
                IBSymbols = self.slist['Symbol'].apply(self.FindIBSymbol)
                
                self.Log('Checking Shares Availability ...')
                 
                if not self.slist.empty:
                    if not (IBSymbols == str(key)).any():
                        self.Log('{} not found in IB shortlist, cannot short'.format(ticker))
                        continue
                    else:
                        row = self.slist[IBSymbols == str(key)]
                        
                        if isinstance(row['AVAILABLE'], str):
                            if row['AVAILABLE'] != '>10000000':
                                if int(row['AVAILABLE']) < shares:
                                    self.Log('{} has not enough shares available'.format(ticker))
                                    continue
                            else:
                                self.Log('row[] not a string, but trying to short anyway')
                
                self.Log('Shorting {} shares of {} at {:.2f} = {:.2f} with {:.2e} DollarVolume'.format(shares,str(key),sec.Price,shares*sec.Price,float(value)))
                                
                oshort = self.MarketOnOpenOrder(key, -shares, asynchronous = True)
                shorted = shorted + 1
                self.openOrders.append(oshort)
                
                if (shorted >= self.numberOfStocks):
                    break
        else:
            shorted = 0
            
            for key,value in StocksToTrade.items():
    
                sec = self.ActiveSecurities[key]
                
                #Due to Margin requirements, take only half
                available = self.Portfolio.TotalPortfolioValue*0.5
                
                #Short Sell out of the gates
                shares = round(available/len(StocksToTrade)/sec.Price)
            
                self.Log('Shorting {} shares of {} at {:.2f} = {:.2f} with {:.2e} DollarVolume'.format(shares,str(key),sec.Price,shares*sec.Price,float(value)))
                
                oshort = self.MarketOrder(key, -shares, asynchronous = True)
                # oshort = self.MarketOrder(key, -shares, asynchronous = True)
                self.openOrders.append(oshort)
                
                if (shorted >= self.numberOfStocks):
                    break

    def OnOrderEvent(self, fill):
        # Short Order Went Through, Issue a Stop Loss
        if (fill.Status == OrderStatus.Filled or fill.Status == OrderStatus.PartiallyFilled) and fill.FillQuantity < 0 and not 'SPY' in str(fill.Symbol):
            
            stopo = self.StopMarketOrder(fill.Symbol, abs(fill.FillQuantity), round(fill.FillPrice*1.2,2))
            self.Log('Setting Stop Loss for {} with Stop {}'.format(fill.Symbol,round(fill.FillPrice*1.2,2)))
            self.stopOrders.append(stopo)
            
            self.Log('Checking Fill Quality ...')
            
        #OrderStatus Invalid and '201 - Order rejected' message => Retry Order
        #Margin Issue, try again to short with half position
        if fill.Status == OrderStatus.Invalid and '201 - Order rejected' in fill.Message:
            o = self.Transactions.GetOrderById(fill.OrderId)
            new_o = self.MarketOrder(o.Symbol, -round(o.Quantity/2), asynchronous = True)
            self.openOrders.append(new_o)
            
        if (fill.Status == OrderStatus.Canceled):
            self.Log('Order cancelled')
    
    def CancelOrders(self):
        for o in self.openOrders:
            if o.Status == OrderStatus.PartiallyFilled or o.Status == OrderStatus.Submitted:
                o.Cancel('Short Order for {} could not be filled completely, cancelling'.format(o.Symbol))
                self.openOrders.remove(o)
    
    def CoverShorts(self):
        
        invested = [ x.Symbol for x in self.Portfolio.Values if x.Invested ]
        
        for s in invested:
            if not str(s) == 'SPY':
                self.Liquidate(s)
        
        for o in self.stopOrders:
            o.Cancel('Cancelling Stop Loss for '+ str(o.Symbol))
            self.stopOrders.remove(o)