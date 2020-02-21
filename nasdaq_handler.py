#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import dropbox
import json


class TransferData:
    def __init__(self, access_token):
        self.access_token = access_token

    def upload_file(self, file_from, file_to):
        """upload a file to Dropbox using API v2
        """
        dbx = dropbox.Dropbox(self.access_token)

        with open(file_from, 'rb') as f:
            dbx.files_upload(f.read(), file_to,mode=dropbox.files.WriteMode.overwrite)

def scrape(event, context):
    url = 'https://api.nasdaq.com/api/marketmovers?assetclass=stocks&exchangeStatus=pre'

    raw = requests.get(url)
    js = json.loads(raw.content)
    
    file_aws = '/tmp/premarket.dat' 

    with open(file_aws, 'w') as outfile: 
        json.dump(js, outfile)

    access_token = '' # Add DropBox Token
    transferData = TransferData(access_token)

    file_from = file_aws
    file_to = '/PreMarket/premarket.json'  # The full path to upload the file to, including the file name

    # API v2
    transferData.upload_file(file_from, file_to)