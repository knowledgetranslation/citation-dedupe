#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import mysql.connector
from lxml import etree, objectify
from lxml.builder import E as buildE
import logging
# import lxml.usedoctest
# import xml.etree.cElementTree as ET
import argparse # optparse
import time
 
PATH = os.path.abspath('.') +'/'
MYSQL_CONFIG = PATH + 'db.cnf'
XML_FILE = PATH + 'data_output_HUGE.xml' # 'CS Lit Search Comp - all [12530].xml' # 'lite_sample.xml'
SCHEMA_FILE = PATH + 'xml.xsd' # 'xml_doll_smart.txt' # 
INTEGER_COLUMNS = ['id']
FILE_EXTENSION  = ['csv', 'xml', 'json']
DUPES_FILE = PATH + 'dupes.' + FILE_EXTENSION[1]
PRETTY_PRINT = False
ORIGINAL_XML_COLUMN = 'xml'
TABLE_COLUMNS =['id', 'reference_type', 'author', 'year', 'title', 'pages', 'secondary_title', 'volume', 'number', 'keywords', 'date', 
'abstract', 'url', 'notes', 'isbn', 'alternate_title', 'accession_number', 'author_address', 'language', 'database_provider', 'name_of_database', ]
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
TABLE_NAME = 'test_upload' #'refs_training' # 'refs' #"test_upload"
tabPK = "id"
LINE_SEPARATOR = '\r' # &#13 '\r\n'

GROUPED_ELEMENTS  = {'key'            : 'foreign-keys',
                    'authors'         : 'contributors',
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
                    
LIST_ELEMENTS   = ['author', 'keyword', 'url']
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

        self.outFile    = outFile
        self.tableName  = tabName
        self.tablePK    = tabPK
        self.limit      = 999999
        self.constraint = ''
        self.columns    = ', '.join(TABLE_COLUMNS)
        self.elements   = {MAP_COLUMN_TO_XML[k] : k for k in MAP_COLUMN_TO_XML}     #Invert into Column-Tag mapping
        self.recCount   = 0
        
        self.con = self.connectDB(MYSQL_CONFIG)
        self.cursor = self.con.cursor(dictionary = True, buffered = False)
        self.cursor.execute("SET net_write_timeout = 3600")

    def startExport(self):
        # data = {}
        res = self.loadData(1000)                                         # Load Data from DB with limitation of output
        data = self.cursor

        if data != None and res != 0:
            logging.info('Data from Table %s were loaded.' % self.tableName)

            doc = self.generateXML()

            if self.writeXmlFile(doc, self.outFile):
                logging.info('Data were successfuly exported into file %s' % self.outFile)
            else:
                logging.warning('Cannot write Data into file %s' % self.outFile)

            self.closeAllConnections
        else:
            logging.warning('Cannot read data from Database Table %s' % self.tableName)
            self.resetExport()

    def startExportFromOriginal(self):
        # data = {}
        self.loadData(self.limit)                                                      # Load Data from DB with limitation of output
        data = self.cursor
        
        if data != None and self.columns in data.description:
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
            print(etree.tostring(data, pretty_print=PRETTY_PRINT, xml_declaration = True, encoding='UTF-8').decode(), file=f) # Python 3.x
            return True
 
        return False

    def addStyle(self, tag, parent=None, content=None):             # Add Style element
        style = buildE(tag)

        if tag == 'style':
            style.attrib['face'] = 'normal'
            style.attrib['font'] = 'default' 
            style.attrib['size'] = '100%'

        if content is not None:
            style.text = str(content)
        if parent is not None:
            parent.append(style)

    def addGroup(self, leafName, parent=None):                      # Add Group's structure
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
    
    def addElement(self, tag, parent=None, content=None):           # Add content data as leaf to branch
        element = buildE(tag)                                       # Create Element
        if content is not None:
            if tag in STYLED_ELEMENTS:                              # Check if needed to add Style element
                self.addStyle('style', element, str(content))
            else:
                element.text = str(content)

        if parent is not None:
            parent.append(element)
        return element

    def generateXML(self):
        doc = self.addElement('xml')
        records = self.addElement('records', parent=doc)
        
        for record in self.cursor.fetchall():
            r = self.addElement('record', parent=records)

            # If posible replace this HARDCODE block
            d = self.addElement('database', content=self.outFile, parent=r)         # Add Tag 'database'
            d.attrib['name'] = self.outFile
            d.attrib['path'] = PATH + self.outFile
            
            s = self.addElement('source-app', content='EndNote', parent=r)          # Add Tag 'source-app'
            s.attrib['name'] = 'EndNote'
            s.attrib['version'] = '17.2'
            
            for k, content in record.items():
                elementName = self.elements.get(k, k)

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
                    else:
                        self.addElement(elementName, branch, str(content))          # Add regular elements

            self.recCount += 1
        return doc

    def loadData(self, limit):
        sql = "SELECT %s FROM %s %s LIMIT %s" % (self.columns, self.tableName, self.constraint, limit)
        
        if self.constraint != '':
            self.cursor.execute("SHOW TABLES LIKE  '%s_entity_map'" % self.tableName)
            # self.cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name ='%s'" % self.tableName)

            if self.cursor.fetchone():
                print(sql)
                self.cursor.execute(sql)
                # return len(self.cursor.fetchall()) 
                return self.cursor.rowcount # & 0xFFFFFFFFFFFFFFFF # Added to solve problem with Wrong rowcount
            else:
                logging.warning("WARNING! Couldn't find table %s_entity_map. Please, run Analyser first." % self.tableName)
                return 0
        else:
            self.cursor.execute(sql)
            return self.cursor.rowcount #& 0xFFFFFFFFFFFFFFFF # Added to solve problem with Wrong rowcount

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
        
        if opt.mode == 'clear':
            export.constraint = ' WHERE %(id)s NOT IN (SELECT %(id)s FROM %(tabName)s_entity_map)' % {'id': export.tablePK, 'tabName': export.tableName}
        elif opt.mode == 'dupes':
            export.constraint = ' WHERE %(id)s IN (SELECT %(id)s FROM %(tabName)s_entity_map)' % {'id': export.tablePK, 'tabName': export.tableName}
        else:
            export.constraint = ''
        
        if opt.fromOriginal:
            export.columns = ORIGINAL_XML_COLUMN
            export.startExportFromOriginal()            # Export from stored Original XML data
        else:
            export.columns = ', '.join(TABLE_COLUMNS)   # Generate columns from a list
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
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify twice for DEBUG)'
                    )

    return parser.parse_args()

def clear_element(element):                             # Clear xml object
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
                    pass
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