#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import mysql.connector
from lxml import etree, objectify
from lxml.builder import E as buildE
import logging
import json

# import lxml.usedoctest
# import xml.etree.cElementTree as ET
import argparse
import time

# FILES VARS
PATH = os.path.abspath('.') +'/'
XML_FILE = PATH + 'output.xml'
SCHEMA_FILE = PATH + 'xml.xsd'

PARAMS_FILE = PATH + 'xml_parameters.json'
MYSQL_CONFIG = PATH + 'db.cnf'

class export(object):
    def __init__(self, outFile = XML_FILE,  tabName=''):
        self.tableName = tabName
        if not self.getParameters(PARAMS_FILE):
            logging.warning('WARNING! Please, choose other Parameters file against %s' % paramsFile)
            self.resetExport()
            # sys.exit(0)
            # quit
            return None
        self.outFile = outFile
        self.constraint = ''
        self.recCount = 0
        self.dupesCount = 0
        
        self.con = self.connectDB(MYSQL_CONFIG)
        self.cursor = self.con.cursor(dictionary = True, buffered = False) # named_tuple = True
        self.cursor.execute("SET net_write_timeout = 3600")

    def jsonExport(self, inFile = None):
        columns = ', '.join(['%s' % self.mapTagToColumn.get(k, k) for k in self.dataColumns]) # if k != 'xml'])
        sql = "SELECT %s FROM %s %s LIMIT %s" % (columns, self.tableName, self.constraint, self.limit)
        # select * from self.tableName _entity_map 
        
        cur = self.cursor
        # inFile = 'test_data.json'
        
        if self.constraint != '':                                                   # Check does it have any constraints related to Analyzer result table
            cur.execute("SHOW TABLES LIKE  '%s_entity_map'" % self.tableName)

            if self.cursor.fetchone():                                              # If related table exists 
                cur.execute(sql)                                            # than run query with constraint
            else:
                logging.warning("WARNING! Couldn't find table %s_entity_map. Please, run Analyser first." % self.tableName)
                return None
        else:
            cur.execute(sql)                                                # For all data run query
        data = cur.fetchall()
        
        if cur != None:
            if inFile != None:
                with open(inFile, 'w') as f:
                    json.dump(data, f, sort_keys = True, indent = 4, ensure_ascii=False)
                
            return json.dumps(data, sort_keys = True, indent = 4, ensure_ascii=False)
        else:
            return None

    def jsonDupesExport(self, inFile = 'test.json'):
        columns = ', '.join(['t.%s' % self.mapTagToColumn.get(k, k) for k in self.dataColumns if k != 'xml'])
        sql = "SELECT %s FROM %s %s LIMIT %s" % (columns, self.tableName, self.constraint, self.limit)
        sql = "SELECT distinct %(id)s FROM %(tabName)s_entity_map where %(id)s <> canon_id" % {'tabName' : self.tableName, 'id' : self.tablePK }
        # select * from self.tableName  
        
        cur = self.cursor
        cur2 = self.cursor
        # inFile = 'test_data.json'
        
        # if self.constraint != '' or True:                                                   # Check does it have any constraints related to Analyzer result table
        cur.execute("SHOW TABLES LIKE  '%s_entity_map'" % self.tableName)

        if self.cursor.fetchone():                                              # If related table exists 
            cur.execute(sql)
            res = cur.fetchall()                                              # than run query with constraint
            data = []
            for row in res:
                sql = "select %(col)s from %(tabName)s t " \
                       "join %(tabName)s_entity_map em on t.%(id)s = em.%(id)s " \
                       "where em.%(id)s = %(idVal)s " \
                       "union select %(col)s from %(tabName)s t " \
                       "join %(tabName)s_entity_map em on t.%(id)s = em.canon_id " \
                       "where em.%(id)s = %(idVal)s" % {'col': columns, 'idVal': row[self.tablePK], 'tabName' : self.tableName, 'id' : self.tablePK } 

                cur2.execute(sql)
                pair = cur2.fetchall()
                if pair is not None and pair !=[] and len(pair)>1:
                    data.append(pair)
                    # data.append({'dupes_count' : len(data)}) # add dupes count
                    # data.update({'dupes_count' : len(data)}) # add dupes count
            self.dupesCount = len(data)
        else:
            logging.warning("WARNING! Couldn't find table %s_entity_map. Please, run Analyser first." % self.tableName)
            return None
        
        if cur2 != None:
            if inFile != None:
                with open(inFile, 'w') as f:
                    json.dump(data, f, sort_keys = True, indent = 4, ensure_ascii=False)
                
            #      return json.dumps(data, sort_keys = True, indent = 4, ensure_ascii=False)
            return data # json.dumps(data, sort_keys = True, pretty_print =False, ensure_ascii=False)
        else:
            return None
                        
    def startExport(self):
        res = self.loadData(self.limit)  # 1000                                       # Load Data from DB with limitation of output
        data = self.cursor

        if data != None and res != 0:
            logging.info('Data from Table %s were loaded.' % self.tableName)

            doc = self.generateXML()

            if self.writeXmlFile(doc, self.outFile):
                logging.info('Data were successfuly exported into file %s' % self.outFile)
            else:
                logging.warning('Cannot write Data into file %s' % self.outFile)

            print(self.jsonExport('test_data.json')) # Export Data in JSON format
            
            self.closeAllConnections
        else:
            logging.warning('Cannot read data from Database Table %s' % self.tableName)
            self.resetExport()

    def startExportFromOriginal(self):
        self.loadData(self.limit)       # 1000                                         # Load Data from DB with limitation of output
        data = self.cursor
        
        if data != None :#and self.columns in data.description:
            logging.info('Original XML data from Table %s were loaded.' % self.tableName)

            with open(self.outFile, 'w') as f:
                print('<?xml version="1.0" encoding="UTF-8" ?><xml><records>', file=f) # Open XML tags
                for xml_string in data.fetchall():
                    print(xml_string[self.columns], file=f)                            # Read original XML from DB
                    self.recCount += 1
                print('</records></xml>', file=f)                                      # Close XML tags
            
                logging.info('Data were successfuly exported into file %s' % self.outFile)
                return True

            logging.warning('Cannot write Data into file %s' % self.outFile)
            self.closeAllConnections
        else:
            logging.warning('Cannot read original XML data from Database Table %s' % self.tableName)
            self.resetExport()
        return False
            
    def writeXmlFile(self, data, outFile):
        with open(outFile, 'w') as f:
            print(etree.tostring(data, pretty_print=self.prettyPrint, xml_declaration = True, encoding='UTF-8').decode(), file=f)
            return True
 
        return False

    def addStyle(self, tag, parent=None, content=None):                             # Add Style element
        style = buildE(tag)

        if tag == 'style':
            for (k, v) in self.tagStyle.items():
                style.attrib[k] = v

        if content is not None:
            style.text = str(content)
        if parent is not None:
            parent.append(style)

    def addGroup(self, leafName, parent=None):                                      # Create Group's structure
        root = None
        childBranch = None
        parentBranch = None
        
        if leafName in self.groupedElements:                                        # If Data grouped
            groupList = [leafName]                                                  # Create Group list initiated from the bottom
            childName = leafName
            
            # Create list of nested groups starting from the bottom
            while True:                                                             # Repeat till a Group has Parent Group
                if childName in self.groupedElements:
                    childName = self.groupedElements[childName]                     # Set Group as Parent Group
                    groupList.append(childName)
                else:
                    break

            rootName = groupList[-1]                                                # Define Root name as the first Top from the end
            if parent is not None:                          
                for p in parent:                                                    # Check if Parent has Element with same Name
                    if p.tag == rootName:
                        root = p                                                    # Set Root as Founded element
                        break
                    else:
                        root = None

            if root is None:
                root = buildE(rootName)                                             # Create Root element

            parentBranch = root                                                     # Set parent as Root

            if len(groupList) > 2:                                                  # If Group has more than one Level
                for childBranchName in groupList[-2:0:-1]:                          # Read Group list starting from second from the end and till first 
                    childBranch = buildE(childBranchName)                           # Create Child element
                    parentBranch.append(childBranch)                                # Add Child to Parent 
                    parentBranch = childBranch                                      # Update Parent into Child
            else:
                childBranch = root                                                  # For one-level Group set Child as Root

        if parent is not None:
            parent.append(root)                                                     # Add Element(child|parent) to Parent(parent|grandParent)
        return childBranch
    
    def addElement(self, tag, parent=None, content=None):                           # Add content data as leaf to branch
        element = buildE(tag)                                                       # Create Element
        if content is not None:
            if tag in self.styledElements:                                          # Check if needed to add Style element
                self.addStyle('style', element, str(content))
            else:
                element.text = str(content)

        if parent is not None:
            parent.append(element)
        return element

    def generateXML(self):
        doc = self.addElement('xml')                                                # Add Tag 'xml'
        records = self.addElement('records', parent=doc)                            # Add Tag 'records'
        
        for record in self.cursor.fetchall():
            r = self.addElement('record', parent=records)                           # Add Main Root element

            d = self.addElement('database', content=self.outFile, parent=r)         # Add Tag 'database'
            d.attrib['name'] = self.outFile
            d.attrib['path'] = PATH + self.outFile
            
            s = self.addElement('source-app', content=self.tagSourceApp['name'], parent=r)  # Add Tag 'source-app' read from parameters
            for k, v in self.tagSourceApp.items():
                s.attrib[k] = v
            
            rec = {k:v for k, v in record.items() if v != None and v !=""}
            
            for k, content in rec.items(): #record.items():
            
                elementName = self.mapColumnToTag.get(k, k) # self.elements.get(k, k)                               # Define element Name as synonym(value) or same(key) from map_column_to_xml dictionary

                # Create tree branches
                if elementName in self.groupedElements:                             # If Element should be in a Group of elements
                    branch = self.addGroup(elementName, parent=r)                   # Create Group Structure
                else:
                    branch = r                                                      # If doesn't have a Group than set as Root

                # Add Data leaves into branches
                if content is not None:
                    if elementName in self.listElements:                            # For elements within string
                        contents = content.split(self.lineSeparator)                # Create List - Fix separator same as in Refs Table!
                        for value in contents:
                            self.addElement(elementName, branch, str(value))        # Add elements from list
                    else:
                        self.addElement(elementName, branch, str(content))          # Add regular elements

            self.recCount += 1
        return doc

    def loadData(self, limit):
        sql = "SELECT %s FROM %s %s LIMIT %s" % (self.columns, self.tableName, self.constraint, limit)
        
        if self.constraint != '':                                                   # Check does it have any constraints related to Analyzer result table
            self.cursor.execute("SHOW TABLES LIKE  '%s_entity_map'" % self.tableName)
            # self.cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name ='%s'" % self.tableName)

            if self.cursor.fetchone():                                              # If related table exists 
                self.cursor.execute(sql)                                            # than run query with constraint
                return self.cursor.rowcount # & 0xFFFFFFFFFFFFFFFF                  # Was added to solve problem with Wrong rowcount
            else:
                logging.warning("WARNING! Couldn't find table %s_entity_map. Please, run Analyser first." % self.tableName)
                return 0
        else:
            self.cursor.execute(sql)                                                # For all data run query
            return self.cursor.rowcount #& 0xFFFFFFFFFFFFFFFF                       # Was added to solve problem with Wrong rowcount

    def getParameters(self, parametersFile):
        """ Define the Performance parameters and Database source table. """
        if os.path.exists(parametersFile):
            logging.info('reading data structure from %s' % parametersFile)
            with open(parametersFile) as df :
                data = json.load(df)

                self.mapTagToColumn = data['map_column_to_xml']
                self.mapColumnToTag = {data['map_column_to_xml'][k] : k for k in data['map_column_to_xml']}
                
                # self.elements = self.mapColumnToTag #{data['map_column_to_xml'][k] : k for k in data['map_column_to_xml']}     #Invert into Column-Tag mapping
                self.groupedElements = data['grouped_elements']
                self.listElements = data['list_elements']
                self.integerColumns = data['integer_columns']
                self.lineSeparator = data['line_separator']

                params = data['source_db']
                self.tableCollate = params['collate']
                self.tablePK = params['tab_PK']
                if self.tableName == '':            # added for web API v1
                    self.tableName = params['tab_name']
                    
                self.dataColumns = data['data_elements']     # List of Tags with meaningful Data
                # self.columns = ', '.join(['%s' % self.mapTagToColumn.get(k, k) for k in dataColumns])
                self.columns = ', '.join([str(self.mapTagToColumn.get(k, k)) for k in self.dataColumns])   # List of columns with meaningful Data
                          
                self.originalXml = params['original_xml_column']
                self.limit = params['row_limit']

                params = data['export']
                self.prettyPrint = params['pretty_print']
                self.styledElements = params['styled_elements']

                params = data['tags']
                self.tagStyle = params['style']
                self.tagSourceApp = params['source-app']

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

    def resetExport(self):
        self.recCount = 0
        self.closeAllConnections

def start(xmlFile = XML_FILE):
    global export
    # initLogging(logging.INFO)
    log_level = logging.WARNING
    start_time = time.time()                    # set start time

    opt = options()
    if opt :
        if opt.verbose :
            if opt.verbose == 1:
                log_level = logging.INFO
            elif opt.verbose >= 2:
                log_level = logging.DEBUG
        initLogging(log_level)                  # start logging
        
        if opt.filename is not None:
            xmlFile = opt.filename
        export = export(xmlFile)                # initiate export
        
        if opt.table is not None:
            export.tableName = opt.table
        
        if opt.mode == 'clear':
            export.constraint = ' WHERE %(id)s NOT IN (SELECT %(id)s FROM %(tabName)s_entity_map)' % {'id': export.tablePK, 'tabName': export.tableName}
        elif opt.mode == 'dupes':
            export.constraint = ' WHERE %(id)s IN (SELECT %(id)s FROM %(tabName)s_entity_map)' % {'id': export.tablePK, 'tabName': export.tableName}
        else:
            export.constraint = ''

        if opt.fromOriginal:
            export.columns = export.originalXml
            export.startExportFromOriginal()            # Export from stored Original XML data
        else:
            # export.columns = export.columns #', '.join(export.columns)  # Generate columns from a list
            export.startExport()                        # Export by generating new XML file
    else:
        initLogging(log_level)                          # Start logging
        export.startExport()                            # Export by generating new XML file
    
    print('%s records were exported' % export.recCount)

def options():                                          # Add parameters
    parser = argparse.ArgumentParser(description='Export Data from Database into XML file. '+
   ' It can be used in Training mode for machinery learning(creating clusters with weight for each similar records pairs). '+
   ' For setup it uses Data Model and loads it from file in .JSON format.'+ 
   ' As output it generates mapping table and stores there all duplicate records.')
    parser.add_argument('filename',  default=XML_FILE)
    parser.add_argument('-m', '--mode', dest='mode', default='clear',  choices=['all', 'clear', 'dupes'], action='store',
                    help='''Set mode for output data into XML file
                    "all"   - output all data (same as input file)
                    "clear" - output clear data without dupes
                    "dupes" - output only duplicated records'''
                    )
    parser.add_argument('-o', '--original', dest='fromOriginal', action='store_true',
                    help='Use it to output XML file from stored original XML data'
                    )
    parser.add_argument('-s', '--source', dest='table',
                    help='Set table Name as source of Data (by Default "Training_data")'
                    )
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify twice for DEBUG)'
                    )

    return parser.parse_args()

def clear_element(element):                             # Clear xml object
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]

def validateXml(xmlFile, schemaFile = SCHEMA_FILE):
    schema = etree.XMLSchema(etree.parse(schemaFile))
    xml_file = etree.parse(xmlFile)
    schema.validate(xml_file)

    # schema = etree.XMLSchema(file=schemaFile)
    # export = objectify.makeparser(schema=schema)
    try:
        with open(xmlFile, 'r') as f:
            etree.fromstring(f.read(), export)
            
        print('File validation was successful')
        return True
    except:
        print('File validation was fail')
        return False
        
def initLogging(log_level = logging.WARNING):
    logging.getLogger().setLevel(log_level)

if __name__ == '__main__':
    start()
    
# def fast_iter(context, cursor):
    # author_list = []
    # paperCounter = 0
    # for event, element in context:
        # if element.tag in CATEGORIES:
            # if len(list(element))>0 and element.tag == 'authors':
                # for author in list(element):
                    # pass
                # print(list(element).getelementpath(element))
            # paper = {
                # 'element' : element.tag,

                # 'dblpkey' : element.get('key')
            # }
            # for data_item in DATA_ITEMS:
                # data = element.find(data_item)
                # if data is not None:
                    # paper[data_item] = data

            # if paper['element'] not in SKIP_CATEGORIES:
                # pass

            # paperCounter += 1
            # clear_element(element)