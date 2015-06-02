#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os.path
import sys
import unittest
# from urlparse import urlparse
# from urllib import parse as urlparse
import urllib
# .parse as urlparse
from urllib.parse import urlparse

from client import ClientAPI
# from mock import patch
# from unittest.mock import patch
import requests
import unittest.mock 
# import mock

PATH = os.path.dirname(__file__)
currentdir = os.path.abspath(PATH)
parentdir = os.path.dirname(currentdir)
sys.path.insert(1,parentdir) 
# sys.path.insert(1, PATH)

# from server 
import server as srv

wrongFileExt = PATH + '/resources/infiles/out.txt'
wrongFileData = PATH + '/resources/infiles/badData.xml'
wrongFileWithNoRecords = PATH + '/resources/infiles/noData.xml'

fileWithOneRecord = PATH + '/resources/infiles/oneRecord.xml'

noDupes = PATH + '/resources/infiles/noDupes.xml'
oneDupe = PATH + '/resources/infiles/out2.xml'
oneDupe = 'out2.xml'

mediumDataFile = PATH + '/resources/infiles/mediumData.xml'
bigDataFile = PATH + '/resources/infiles/bigData.xml'

dupeArticle = PATH
dupeAuthors = PATH
dupeAbstract = PATH
dupeAccessNumber = PATH
dupeISBN = PATH

# SERVER_IP =  'http://0.0.0.0:8081' #'http://45.55.160.201'
# SERVER_IP =  'http://45.55.160.201:8088'
SERVER_IP =  'http://45.55.160.201'

tableName = 'training_data'
testFile = oneDupe

urlVersion = "%s/ver" % SERVER_IP
urlUpload = "%s/v1/upload" % SERVER_IP

# urlStop = "%s/stop" % SERVER_IP

# wrongMethodCurlUpload = "curl -X GET -F file=@%s %s/v1/upload" %(testFile, SERVER_IP)
# curlUpload = "curl -X POST -F file=@%s %s/v1/upload" %(testFile, SERVER_IP)

# curlParse = "curl %s/v1/analyze" % SERVER_IP
# curlParseDataIntoTable = "curl %s/v1/analyze/%s" %(SERVER_IP, tableName)

# curlAnalysis = "curl %s/v1/parse" % SERVER_IP
# curlAnalysisDataInTable = "curl %s/v1/parse/%s" %(SERVER_IP, tableName)

# curlExport = "curl %s/v1/export" % SERVER_IP
# curlExportFromTable = "curl %s/v1/export/%s" %(SERVER_IP, tableName)

# wrongMethodCurlTraining = "curl -X GET -F file=@%s %s/v1/training" %(testFile, SERVER_IP)
# curlTraining = "curl -X POST -F file=@%s %s/v1/training" %(testFile, SERVER_IP)

def urlResponse(url, fileName = ''):
    """
    A stub urlopen() implementation that load json responses from
    the filesystem.
    """
    # Map path from url to a file
    # if fileName != '':
        # resource_file = os.path.abspath('resources/responses%s.json' % fileName)
    # else:
        # parsed_url = urlparse(url)
        # resource_file = os.path.abspath('resources/responses%s.json' % parsed_url.path)    

    # resource_file = fileName or parsed_url.path
    parsed_url = urlparse(url)
    resource_file = os.path.abspath('resources/responses/v1/%s.json' % (fileName or parsed_url.path))
 
    with open(resource_file, 'r') as f:    
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

    @unittest.skip("Skip Demo Test")        
    def test_getApiVersion_OK(self):
        # Arrange
        url = urlVersion
        expected = urlResponse(url)
        
        # Act
        response = requests.get(url)

        # Assert
        self.assertIn('data', response.json())
        self.assertEqual(response.json(), expected)

    @unittest.skip("Skip Big Data file")  
    def test_postUploadBig_OK(self):
        # Arrange
        url = urlUpload
        expected = urlResponse(url)
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

    @unittest.skip("Skip Medium Data file")
    def test_postUploadMedium_OK(self):
        # Arrange
        url = urlUpload
        dataFile = 'mediumData.xml'
        expected = urlResponse(url, 'mediumData')
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)
            r = response.json() #['status']
        
            # Assert
            self.assertIn('data', r)
            self.assertEqual(r['status'], expected['status'])
            self.assertEqual(r['code'], expected['code'])
            
            self.assertEqual(response.json(), expected)
            
        # print(r['data'])
        # print('**********************************')
        # print('@@@', len(r['data']))
        # print('###', len(r['dupes']))
            
        self.assertEqual(len(r['dupes']), 542) # 538) # len(expected['dupes']))
        self.assertEqual(len(r['data']), 4005997) # 12530 len(expected['data']))

    def test_getUpload_502Error(self):
        # Arrange
        url = urlUpload
        dataFile = 'mediumData.xml'
        expected = urlResponse(url, 'getUpload502Error')
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.get(url=url, files=files)
            r = response #.json() #['status']

            # Assert
            self.assertEqual(r.status_code, 502)

    def test_postUpload_415Error(self):
        # Arrange
        url = urlUpload
        dataFile = 'out.txt'
        expected = urlResponse(url, 'postUpload415Error')
         
        with open(PATH + '/resources/infiles/'+ dataFile, 'rb') as f:
            files = {'file': f, 'filename': dataFile, }
        
            # Act
            response = requests.post(url=url, files=files)

            # Assert
            self.assertEqual(response.status_code, 415)
            self.assertEqual(response.json(), expected)

    def test_v1_anylink_5009Error(self):
        # Arrange
        url = "%s/anylink" % SERVER_IP
        expected = urlResponse(url, 'getUpload5009Error')

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), expected)

    def test_staticPageAnylink_404Error(self):
        # Arrange
        url = "%s/v1/anylink" % SERVER_IP

        # Act
        response = requests.get(url)

        # Assert
        self.assertEqual(response.status_code, 404)




"""
1) error wrong data file
    1.1 A properly formed request with no records should return ….
        
    1.2 A properly formed request with damage data should return ….
        
    1.3 A properly formed request with wrong extension
+            415	Unsupported Media Type

2) no error, Warning!
    2.1 A properly formed request with one record should return ….
            Warning! Data file contains only one record, analysis couldn't be continue.

3) An invalid request should return ….
+            error 404 Not Found

4) A properly formed request with 6 records and 2 dupes should return
    no error, len(data) = 6,  len(dupes) = 2
+    4.1 Big Data
+    4.2 Medium Data
"""
        
"""
    r = requests.get(SERVER_URL + "/posts/count", cookies=cookies)
    n_original_posts = int(r.json()['count'])
            
            
    def test_file_upload(self):
        expected = {"data": "1", 
                    "details": "http://0.0.0.0:8081/ver", 
                    "status": 200, 
                    "message": "Deduper service API version: 1"}
                    
        res = self.srv.get('/ver')
        # test = testDeduper(self.user, self.userActionList, 'get')

        # self.assertEqual(test, True)
        self.assertEqual(json.loads(res.data), expected)
"""        

if __name__ == '__main__':
    unittest.main()