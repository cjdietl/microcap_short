import dropbox
import numpy
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import dropbox

from ftplib import FTP

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
    url = 'ftp3.interactivebrokers.com'

    ftp = FTP(url)
    ftp.login('shortstock','')

    ftp.retrbinary('RETR usa.txt' , open('/tmp/ibshortlist.dat', 'wb').write)

    access_token = 'DbS1bNZfzgAAAAAAAAAODOrtaYRW4ahRGk-1AievhNpX5E-1D97_o1yzw--3h1uy'
    transferData = TransferData(access_token)

    file_from = '/tmp/ibshortlist.dat'
    file_to = '/IB_Shortlist/ibshortlist.dat'  # The full path to upload the file to, including the file name

    # API v2
    transferData.upload_file(file_from, file_to)