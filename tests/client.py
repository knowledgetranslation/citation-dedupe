#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
# from urllib2 import urlopen
# from urllib.request import urlopen
import urllib
# .request

class ClientAPI(object):
    def request(self, url='http://45.55.160.201/ver'):
        response = urlopen(url)

        raw_data = response.read().decode('utf-8')
        return json.loads(raw_data)