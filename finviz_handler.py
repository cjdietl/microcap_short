#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import dropbox
import numpy
import datetime

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

    session = requests.Session()

    data = {'email':'', 'password':'','remember':'true'} # Insert your FinViz Login
    url = 'https://finviz.com/login_submit.ashx'
    headers={"User-Agent":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36"}

    session.headers.update(headers)
    r = session.post(url, data=data)

    print(session.cookies)

    r=session.get('https://elite.finviz.com/export.ashx?v=111&f=sh_price_u5,ta_change_u5&ft=4&o=-change', allow_redirects=True)

    fileaws_tmp = open('/tmp/finviz_tmp.csv', 'w+b')
    fileaws_tmp.write(r.content)
    fileaws_tmp.close()

    fileaws_tmp = open('/tmp/finviz_tmp.csv', 'r')
    contents = fileaws_tmp.read()
    fileaws_tmp.close()

    fileaws = open('/tmp/finviz.csv', 'w+')
    fileaws.write('#{}\n'.format(datetime.datetime.utcnow()))
    fileaws.write(contents)
    fileaws.close()

    access_token = '' # Include Token for Dropbox
    transferData = TransferData(access_token)

    file_from = '/tmp/finviz.csv'
    file_to = '/FinViz/finviz.csv'

    transferData.upload_file(file_from, file_to)