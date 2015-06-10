#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os.path
import sys
import unittest
import urllib
import requests
from urllib.parse import urlparse
import datetime

# import unittest.mock 
# from client import ClientAPI

PATH = os.path.dirname(__file__)
currentdir = os.path.abspath(PATH)
parentdir = os.path.dirname(currentdir)
sys.path.insert(1,parentdir) 
# sys.path.insert(1, PATH)

# from server 
import server as srv

dupeArticle = PATH
dupeAuthors = PATH
dupeAbstract = PATH
dupeAccessNumber = PATH
dupeISBN = PATH

# SERVER_IP =  'http://45.55.160.201:8088'
SERVER_IP =  'http://45.55.160.201'
tableName = 'training_data'
urlUpload = "%s/v1/upload" % SERVER_IP

# r = requests.get("https://a.b.c:7895/resource/path?param1=foo",auth=(username,password),verify=False)

def urlResponse(url, fileName = ''):
    parsedUrl = urlparse(url)
    resourceFile = os.path.abspath('resources/responses/v1/%s.json' % (fileName or parsedUrl.path))
 
    with open(resourceFile, 'r') as f:    
        data = json.load(f)
    return data

class testDeduper(unittest.TestCase):    
    def setUp(self):
        # self.patcher = patch('server.upload', fake_urlopen)
        # self.patcher.start()
        # self.client = ClientAPI()
        # srv.start()
        # srv.start(True)
        pass

    def tearDown(self):
        # print(srv.stop)
        # srv.stop()
        # self.patcher.stop()
        pass

    # @unittest.skip("Demo Test")        
    def test_getApiVersion_OK(self):
        # Arrange
        url = "%s/ver" % SERVER_IP
        expected = urlResponse(url)
        
        # Act
        response = requests.get(url)

        # Assert
        self.assertIn('data', response.json())
        self.assertEqual(response.json(), expected)

    @unittest.skip("Big Data file")  
    def test_postUploadBig_OK(self):
        # Arrange
        url = urlUpload
        expected = urlResponse(url, 'bigData')
        dataFile = 'bigData.xml'
        
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)
            r = response.json() #['status']
        
            # Assert
            self.assertIn('data', r)
            self.assertEqual(r['status'], expected['status'])
            self.assertEqual(r['code'], expected['code'])
            
            self.assertEqual(len(r['data']), 12530) #len(expected['data']))
            self.assertEqual(len(r['dupes']), 538) # len(expected['dupes']))

    # @unittest.skip("Medium Data file")
    def test_postUploadMedium_OK(self):
        # Arrange
        url = urlUpload
        expected = urlResponse(url, 'mediumData')
        dataFile = 'mediumData.xml'
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)
            r = response.json() #['status']
        
            # Assert
            self.assertIn('data', r)
            self.assertEqual(r['status'], expected['status'])
            self.assertEqual(r['code'], expected['code'])
            
            # self.assertEqual(response.json(), expected)
            
        # self.assertEqual(len(r['data']), 4005997) # 12530 len(expected['data']))
        self.assertEqual(len(r['dupes']), 542) # 538) # len(expected['dupes']))

    # @unittest.skip("Wrong Method")
    def test_getUpload_405Error(self):
        # Arrange
        url = urlUpload
        dataFile = 'oneRecord.xml'
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.get(url=url, files=files)

            # Assert
            self.assertEqual(response.status_code, 405)

    # @unittest.skip("Wrong File type")
    def test_postUploadWrongExtension_415Error(self):
        # Arrange
        url = urlUpload
        dataFile = 'out.txt'
        expected = urlResponse(url, 'postUpload415Error')
        # print('###', PATH + '/resources/infiles/'+ dataFile)
        
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)

            # Assert
            self.assertEqual(response.status_code, 415)
            self.assertEqual(response.json(), expected)

    # @unittest.skip("Wrong Link")
    def test_v1_anylink_5009Error(self):
        # Arrange
        url = "%s/anylink" % SERVER_IP
        expected = urlResponse(url, 'getAnylink5009Error')

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), expected)

    # @unittest.skip("Wrong main page")
    def test_staticPageAnylink_404Error(self):
        # Arrange
        url = "%s/v1/anylink" % SERVER_IP

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(response.status_code, 404)

    # @unittest.skip("Warning! Data contains only one record")
    def test_postUploadOneRecord_2001Warning(self):
        # Arrange
        url = urlUpload
        dataFile = 'oneRecord.xml'
        expected = urlResponse(url, 'postUpload2001Warning')
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)

            # Assert
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), expected)

    # @unittest.skip("Wrong Data file")
    def test_postUploadWrongData_5006Error(self):
        # Arrange
        url = urlUpload
        expected = urlResponse(url, 'wrongData')
        dataFile = 'wrongData.xml'
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)
            # r = response.json() #['status']
            # print(response.json())
            # Assert   
            self.assertEqual(response.json(), expected)

    # @unittest.skip("Test Export File")
    def test_exportFile(self):
        # Arrange
        dataFile = 'liteData.xml'
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=urlUpload, files=files)
            r = response.json() #['status']
        
            # Assert
            self.assertEqual(r['status'], 200)
            # self.assertEqual(len(r['dupes']), 2) 
        
        # Arrange
        tableName = '%s_%s' % (dataFile.split('.')[0], str(datetime.date.today()).replace('-', ''))
        urlExport = "%s/v1/export/%s/True" % (SERVER_IP, tableName)
        response = requests.get(url=urlExport)        
        outFile = '%s.xml' % tableName
        
        with open(parentdir +'/tmp/'+ outFile, 'rb') as f:
            files = {'file': f, 'filename': outFile, }
        
            # Act
            response = requests.post(url=urlUpload, files=files)
            r = response.json() #['status']

            # Assert
            # self.assertIn('data', r)
            self.assertEqual(r['status'], 200)
            # self.assertEqual(r['code'], expected['code'])
            
            self.assertEqual(len(r['dupes']), 0)
    
if __name__ == '__main__':
    unittest.main()
    
    
    
    