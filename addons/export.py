#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import mysql.connector
from lxml import etree, objectify
from lxml.builder import E as buildE
import logging
# import lxml.usedoctest
# import xml.etree.cElementTree as ET

 
PATH = os.path.abspath('.') +'/'
MYSQL_CONFIG = PATH + 'db.cnf'
XML_FILE = PATH + 'data_output_HUGE.xml' # 'CS Lit Search Comp - all [12530].xml' # 'lite_sample.xml'
SCHEMA_FILE = PATH + 'xml.xsd' # 'xml_doll_smart.txt' # 
INTEGER_COLUMNS = ['id']
FILE_EXTENSION  = ['csv', 'xml', 'json']
DUPES_FILE = PATH + 'dupes.' + FILE_EXTENSION[1]
PRETTY_PRINT = False
TABLE_COLUMNS =['id', 'reference_type', 'author', 'year', 'title', 'pages', 'secondary_title', 'volume', 'number', 'keywords', 'date', 
'abstract', 'url', 'notes', 'isbn', 'alternate_title', 'accession_number', 'author_address', 'language', 'database_provider',
'name_of_database', ]
"""
database
source-app
    key
ref-type
author
auth-address
title
secondary-title
alt-title
    periodical
        full-title
        abbr-1
    alt-periodical
        full-title
        abbr-1
pages
volume
number
keyword
year
date
isbn
accession-num
abstract
notes
url
remote-database-name
remote-database-provider
language
"""

CLEAR_DATA_FILE = PATH + 'clear_data.' + FILE_EXTENSION[1]
TABLE_NAME = 'refs' #'refs_training' # 'refs' #"test_upload"
tabPK = "id"
LINE_SEPARATOR = '\r' # &#13 '\r\n'

GROUPED_ELEMENTS  = {'key'            : 'foreign-keys',
                    'authors'         : 'contributors',
                   # 'contributors'    : 'test',
                    'author'          : 'authors',
                    'keyword'         : 'keywords',
                    'title'           : 'titles',
                    'secondary-title' : 'titles', 
                    'alt-title'       : 'titles',
                    'full-title'      : 'periodical',
                    'abbr-1'          : 'periodical',
                    'year'            : 'dates',
                    'pub-dates'       : 'dates',
                    'date'            : 'pub-dates',
                    'related-urls'    : 'urls',
                    'url'             : 'related-urls'}
                    
LIST_ELEMENTS   = ['author', 'keyword', 'url'] #, 'title', 'secondary-title', 'alt-title']
STYLED_ELEMENTS = ['author', 'auth-address', 'title', 'secondary-title', 'alt-title', 'full-title', 'abbr-1', 'pages', 'volume', 'number', 'keyword', 'year', 'date', 'isbn', 'accession-num', 'abstract', 'notes', 'url', 'remote-database-name', 'remote-database-provider', 'language']

# commented for same names in pair Tag-Column
MAP_COLUMN_TO_XML = { # 'database'
                      # 'source-app'
                      # 'key'
                        'rec-number'               : 'id', 
                        'ref-type'                 : 'reference_type',
                      #  'author'                   : 'author', 
                        'auth-address'             : 'author_address',
                      #  'title'                    : 'title', 
                        'secondary-title'          : 'secondary_title', 
                        'alt-title'                : 'alternate_title', 
                      #  'full-title'               : 'per-full-title'
                      #  'abbr-1'                   : 'per-abbr-1',
                      #  'full-title'               : 'alt-full-title'
                      #  'abbr-1'                   : 'alt-abbr-1',
                      #  'pages'                    : 'pages', 
                      #  'volume'                   : 'volume', 
                      #  'number'                   : 'number', 
                        'keyword'                  : 'keywords',
                      #  'year'                     : 'year', 
                      #  'date'                     : 'date',
                      #  'isbn'                     : 'isbn', 
                        'accession-num'            : 'accession_number', #'electronic_resource_number',
                      #  'abstract'                 : 'abstract', 
                        'notes'                    : 'research_notes',
                      #  'url'                      : 'url',
                        'remote-database-name'     : 'name_of_database',
                        'remote-database-provider' : 'database_provider'#,
                      #  'language'                 : 'language'               
                        }

class export(object):
    def __init__(self, outFile = XML_FILE, tabName = TABLE_NAME):
        # if not self.getParameters(xmlFile):
            # logging.warning('WARNING! Please, choose other Data Model file against %s' % xmlFile)
            # self.resetAnalyzer()
            # sys.exit(0)

        self.outFile = outFile
        self.tableName = tabName
        self.recCount = 0
        self.fields = {MAP_COLUMN_TO_XML[k] : k for k in MAP_COLUMN_TO_XML}     #Invert into Column-Tag mapping
        
        self.con = self.connectDB(MYSQL_CONFIG)
        self.cursor = self.con.cursor(dictionary = True, buffered = False)
        self.cursor.execute("SET net_write_timeout = 3600")

    def startExport(self):
        data = {}
        self.loadData()
        data = self.cursor
        print(', '.join(TABLE_COLUMNS))
        
        if data != None:
            logging.info('Data from Table %s were loaded.' % self.tableName)
            
            doc = self.fetchXML() #data)

            if self.writeXmlFile(doc, self.outFile):
                logging.info('Data were successfuly exported into file %s' % self.outFile)
            else:
                logging.warning('Cannont write Data into file %s' % self.outFile)

            self.closeAllConnections
        else:
            logging.warning('Cannont read data from Database Table %s' % self.tableName)
            self.resetExport()
            
    def writeXmlFile(self, data, outFile):
        with open(outFile, 'w') as f:
            print(etree.tostring(data, pretty_print=PRETTY_PRINT, xml_declaration = True, encoding='UTF-8').decode(), file=f) # Python 3.x
            return True
 
        return False
            
        # schema = etree.XMLSchema(file=SCHEMA_FILE)
        # export = objectify.makeparser(schema=schema)
    def addStyle(self, tag, parent=None, content=None): # or addSubElement
        style = buildE(tag)

        if tag == 'style':
            style.attrib['face'] = 'normal'
            style.attrib['font'] = 'default' 
            style.attrib['size'] = '100%'

        if content is not None:
            style.text = str(content)
        if parent is not None:
            parent.append(style)
        # return style

    def addGroup(self, leafName, parent=None):
        root = None
        childBranch = None
        parentBranch = None
        
        if leafName in GROUPED_ELEMENTS:                    # If Data grouped
            groupList = [leafName]                          # Create Group list initiated from the bottom
            childName = leafName
            
            # Create list of nested groups starting from the bottom
            while True:
                if childName in GROUPED_ELEMENTS:
                    childName = GROUPED_ELEMENTS[childName]
                    groupList.append(childName)
                else:
                    break

            rootName = groupList[-1]                        # Define Root name as the first Top from the end
            if parent is not None:                          
                for p in parent:                            # Check if Parent hasElement with same Name
                    if p.tag == rootName:
                        root = p                            # Set Root as Founded element
                        break
                    else:
                        root = None

            if root is None:
                root = buildE(rootName)                     # Create Root element

            parentBranch = root                             # Set parent as Root

            if len(groupList) > 2:                          # If Group has more than one Level
                for childBranchName in groupList[-2:0:-1]:  # Read Group list starting from second from the end and till first 
                    childBranch = buildE(childBranchName)   # Create Child element
                    parentBranch.append(childBranch)        # Add Child to Parent 
                    parentBranch = childBranch              # Update Parent into Child
            else:
                childBranch = root                          # For one-level Group set Child as Root

        if parent is not None:
            parent.append(root)                             # Add Element(child|parent) to Parent(parent|grandParent)
        return childBranch
    
    # Add Leaf-data to branch
    def addElement(self, tag, parent=None, content=None):
        element = buildE(tag)                               # Create Element
        if content is not None:
            if tag in STYLED_ELEMENTS:                      # Check if needed to add Style element
                self.addStyle('style', element, str(content))
            else:
                element.text = str(content)

        if parent is not None:
            parent.append(element)
        return element

    def fetchXML(self):
        doc = self.addElement('xml')
        records = self.addElement('records', parent=doc)

        for record in self.cursor.fetchall():
            r = self.addElement('record', parent=records)
            # If posible replace this HARDCODE block
            d = self.addElement('database', content=self.outFile, parent=r)
            d.attrib['name'] = self.outFile
            d.attrib['path'] = PATH + self.outFile
            
            s = self.addElement('source-app', content='EndNote', parent=r)
            s.attrib['name'] = 'EndNote'
            s.attrib['version'] = '17.2'
            
            for k, content in record.items():
                elementName = self.fields.get(k, k)

                # Create tree branches
                if elementName in GROUPED_ELEMENTS:                                 # If Element should be in Group of elements
                    branch = self.addGroup(elementName, parent=r)                   # Create Group Structure
                else:
                    branch = r                                                      # If isn't Group and to root

                # Add Data leaves into branches
                if content is not None:
                    if elementName in LIST_ELEMENTS:                                # For elements within string
                        contents = content.split(LINE_SEPARATOR)                    # Create List - Fix separator same as in Refs Table!
                        for value in contents:
                            self.addElement(elementName, branch, str(value))        # Add elements from list
                            # print(value)
                    else:
                        self.addElement(elementName, branch, str(content))          # Add regular elements

            self.recCount += 1
        return doc

    def loadData(self, limit = 1000):
        q = "SELECT %s FROM %s t LIMIT %s" % (', '.join(TABLE_COLUMNS), self.tableName, limit)
        self.cursor.execute(q)
        return self.cursor.rowcount

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
    initLogging(logging.INFO)
    global export
    export = export(xmlFile)       # initiate analyser
    export.startExport()
    print('%s records were exported' % export.recCount)
    # schema = etree.XMLSchema(file=SCHEMA_FILE)
    # export = objectify.makeparser(schema=schema)
    
    # if validateXml(CLEAR_DATA_FILE, export):
        # print("URA!")
            
# clear xml object
def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]

def fast_iter(context, cursor):

    author_list = []
    paperCounter = 0
    for event, element in context:
        if element.tag in CATEGORIES:
            # for authors in element.find("authors"):# element.findall("authors"):
            if len(list(element))>0 and element.tag == 'authors':
                for author in list(element):
                    print(author.text)
                print(list(element).getelementpath(element))
            paper = {
                'element' : element.tag,
                # 'mdate' : element.get("mdate"),
                'dblpkey' : element.get('key')
            }
            for data_item in DATA_ITEMS:
                data = element.find(data_item)
                if data is not None:
                    paper[data_item] = data

            if paper['element'] not in SKIP_CATEGORIES:
                # insertIntoDB()
                # populate_database(paper, authors, cursor)
                # print(author_list)
                pass

            paperCounter += 1

            clear_element(element)

# tree = etree.ElementTree(root)
# for e in root.iter():
    # print tree.getpath(e)

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
    start('data_output.xml')