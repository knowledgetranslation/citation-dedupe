#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import mysql.connector
from lxml import etree, objectify
import logging
import json
# import lxml.usedoctest
# import xml.etree.cElementTree as ET

# FILES VARS
PATH = os.path.abspath('.') +'/'
XML_FILE = PATH + 'lite_sample4.xml'    # 'CS Lit Search Comp - all [12530].xml' # 'lite_sample.xml'
SCHEMA_FILE = PATH + 'xml.xsd'          # 'xml_doll_smart.txt' #

PARAMS_FILE = PATH + 'xml_parameters.json'
MYSQL_CONFIG = PATH + 'db.cnf'


class parser(object):
    def __init__(self, inFile = XML_FILE, tabName ='', paramsFile=PARAMS_FILE):
        self.tableName = tabName
        if not self.getParameters(paramsFile):
            logging.warning('WARNING! Please, choose other Parameters file against %s' % paramsFile)
            self.resetExport()
            # sys.exit(0)
            quit
            return None

        # print( self.tableName)
        self.recCount = 0
        self.xmlFile = inFile

        self.con = self.connectDB(MYSQL_CONFIG)
        self.cursor = self.con.cursor(buffered = False)
        self.cursor.execute("SET net_write_timeout = 3600")

    def startParse(self):
        xml = self.loadXml(self.xmlFile, SCHEMA_FILE)

        if xml != None:
            logging.info('Creating Data table')
            self.createDataTable()                  # Create Data table
            
            logging.info('Start parsing')
            isParsed = self.parseXml(xml)                      # Parsing XML and load into Data table

            if isParsed:
                logging.info('Data uploaded into DB')
                self.closeAllConnections()
                return True
            else:
                logging.warning('Error! Data wasn\'t uploaded into DB')
                self.closeAllConnections()
                return False
        else:
            logging.warning('Cannot read XML data from %s' % XML_FILE)
            self.resetParser()
            return False

    def loadXml(self, inFile, schemaFile = SCHEMA_FILE):
        # schema = etree.XMLSchema(file=schemaFile)
        # parser = objectify.makeparser(schema=schema)

        if True:# validateXml(inFile, parser):
            # doctree = objectify.parse(inFile, parser=parser)
            doctree = objectify.parse(inFile)
            root = doctree.getroot()
            return root
        else:
            return None

    def parseXml(self, xmlObject):
        data = {}
        columnMapInverted = {self.mapColumnToXml[k] : k for k in self.mapColumnToXml}
        
        for r in xmlObject.records.record:
            d = dict((k,'') for k in self.dataElements)             # Create dictionary for
            
            for e in r.getchildren():
                if e.tag in self.dataElements:
                    if e.text is not None:
                        d[e.tag] = e.text
                    else:              
                        try:
                            # d[e.tag] = str(e.getchildren()[0]).encode('unicode-escape')
                            d[e.tag] = e.findtext('*').encode('unicode-escape') 
                        except:
                            d[e.tag] = ''

                elif e.tag in self.groupedElements.values():                                # Check if element is Parent for Group
                    values_list = []
                    
                    for s in e.getchildren():
                        if s.tag in [self.groupedElements[v] for v in self.listElements]:   # Check if element within Group should be in a List
                            for aa in s.getiterator(self.listElements):                     # Select the elements which should be in a List
                                values_list.append(aa.findtext('*'))                        # Get first text in tag 'style' element

                            d[aa.tag] = self.lineSeparator.join(values_list)                # Set Column value = List converted into string with line separator

                        elif s.tag in self.dataElements:
                            # if s.hasattr 
                                try:             
                                    d[s.tag] = s.findtext('*').encode('unicode-escape') 
                                    # d[s.tag] = str(s.getchildren()[0]).encode('unicode-escape') 
                                except:
                                    d[s.tag] = ''

            self.loadDataIntoDb(d, r)
            self.recCount += 1
        return True

    def loadDataIntoDb(self, dataObject, xml): 
        q_columns = ''
        q_empty = ''
        
        # Generate Columns list for insert into table
        q_columns = ', '.join(['%s' % self.mapColumnToXml.get(k, k) for k in dataObject.keys()])

        # Generate '%s' list for parametric insert into table
        q_empty = ', '.join(['%s' for k in dataObject.keys()])
        
        values = list(dataObject.values())
        
        if self.saveOriginalXml:
            q_columns += ', %s' % self.originalXml
            q_empty += ', %s'
            values.append(etree.tostring(xml))

        q = "insert into %s (%s) values(%s)" % (self.tableName, q_columns, q_empty) # Prepare sql query
        self.cursor.execute (q, values)                                             # Run sql query
        self.con.commit()

    def createDataTable(self):
        tabFields = ''

        # Generate Columns list for new Table
        tabFields = ', '.join(['%s %s' % (field, 'INTEGER' if field in self.integerColumns else 'MEDIUMTEXT') for field in self.columns]) #fields.split(', ')])

        q = "DROP TABLE IF EXISTS %s" % self.tableName
        self.cursor.execute(q)

        q = "CREATE TABLE %s (%s) CHARACTER SET utf8 COLLATE %s" % (self.tableName, tabFields, self.tableCollate)
        self.cursor.execute(q)

    def getParameters(self, parametersFile):
        """ Define the Performance parameters and Database source table. """
        if os.path.exists(parametersFile):
            logging.info('reading data structure from %s' % parametersFile)
            with open(parametersFile) as df :
                data = json.load(df)

                params = data['source_db']
                self.tableCollate = params['collate']
                if self.tableName == '':             # added for web API v1
                    self.tableName = params['tab_name']
                self.tablePK = params['tab_PK']
                self.columns = params['tab_columns'] # ENDNOTE_COLUMNS
                self.originalXml = params['original_xml_column']

                self.saveOriginalXml = data['parser']['save_original_xml']
                 
                self.groupedElements = data['grouped_elements']
                self.mapColumnToXml = data['map_column_to_xml']
                self.listElements = data['list_elements']
                self.dataElements = data['data_elements']
                self.integerColumns = data['integer_columns']
                self.lineSeparator = data['line_separator']

                return True
        else: 
            logging.warning('WARNING! Could not read Parameters from %s' % parametersFile)
            return False

    def connectDB(self, dbOptionsFile):
        """ You need to fill option file `db.cnf` with your mysql database information """
        return mysql.connector.connect(option_files = dbOptionsFile)

    def closeAllConnections(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
            
        if hasattr(self, 'con'):
            self.con.close()

    def resetParser(self):
        self.recCount = 0
        self.closeAllConnections
        
def start(xmlFile = XML_FILE):
    global parser
    log_level = logging.WARNING
    initLogging(log_level)
    
    print('Parsering in progress...')
    parser = parser(xmlFile)       # initiate analyser
    parser.startParse()
    print('%s records were uploaded' % parser.recCount)

def reset():
    parser.resetParser()

def initLogging(log_level = logging.WARNING):
    logging.getLogger().setLevel(log_level)

def validateXml(xmlFile, parser):
    # schema = etree.XMLSchema(file=schemaFile)
    # parser = objectify.makeparser(schema=schema)
    try:
        with open(xmlFile, 'r') as f:
            etree.fromstring(f.read(), parser)

            logging.info('File validation was successful.')
            return True
    except:
        logging.warning('WARNING! File %s validation was fail.' % parametersFile)
        return False
    # return objectify.parse(xmlFile, parser=parser)

if __name__ == '__main__':
    start('lite_sample4.xml') #'CS Lit Search Comp - all [12530].xml')