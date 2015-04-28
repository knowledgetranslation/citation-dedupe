#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
""" This Module analyses Data from Database for duplicate records.
 It used Training method to create block of "options" for Active learning.
 It loads Data Model from file in JSON format. As output it generates
 mapping table for all duplicate records.

 __Note:__ To create related tables in Database you need to run `/init_db.py` 
 before running this script. """
 
# from __future__ import print_function

import os, sys
import itertools
import logging
# import locale
# import pickle
# import multiprocessing

import mysql.connector
import dedupe
import json

from addons import trainer

# FILES VARS
PATH = os.path.abspath('.') +'/'
MODEL_FILE = PATH + 'data_model.json'

MYSQL_CONFIG = PATH + 'db.cnf'
SETTINGS_FILE = PATH + 'dedupe_settings'
TRAINING_FILE = PATH + 'dedupe_training.json'

class analyzer(object):
    def __init__(self, modelFile = MODEL_FILE):
        if not self.getParameters(modelFile):
            logging.warning('WARNING! Please, choose other Data Model file against %s' % modelFile)
            self.resetAnalyzer()
            sys.exit(0)

        self.dupesCount = 0
        self.recordsCount = 0
        self.modelFile = modelFile

        self.con = self.connectDB(MYSQL_CONFIG)
        self.con2 = self.connectDB(MYSQL_CONFIG)
        
        self.dictCursor = self.con.cursor(dictionary = True, buffered = False)
        self.tupleCursor = self.con2.cursor(named_tuple = True, buffered = False)

        self.dictCursor.execute("SET net_write_timeout = 3600")
        self.tupleCursor.execute("SET net_write_timeout = 3600")
        self.deduper = ()
        
    def startAnalyze(self, forceTraining = False):
        # if self.isActiveLearning:
        if forceTraining:
            global trainer

            trainer = trainer.trainer(MODEL_FILE) #self.modelFile)
            self.deduper = trainer.startTraining()

        else:
            logging.info('reading trainnig settings from %s' % SETTINGS_FILE)
            if os.path.exists(SETTINGS_FILE):
                with open(SETTINGS_FILE, 'rb') as sf : 
                    self.deduper = dedupe.StaticDedupe(sf, num_cores = self.cpu)
            else:
                logging.warning('WARNING! Could not find file with trainnig settings, please teach system first')
                sys.exit(0)

        self.startBlocking(self.deduper) #, self.step_size)
        clustered_dupes = self.startClustering(self.deduper)
        logging.info('Clustering is finished')

        self.dupesCount = len(clustered_dupes) # set the number of founded duplicates 
        self.saveResults(clustered_dupes)
        self.closeAllConnections

    def connectDB(self, dbOptionsFile):
        """ You need to fill option file `db.cnf` with your mysql database information """
        return mysql.connector.connect(option_files = dbOptionsFile)

    def dbWriter(self, sql, rows) :
        """ We will also speed up the writing by of blocking map by using 
         parallel database writers """
        conn = self.connectDB(MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        cursor.close()
        conn.commit()
        conn.close()

    def startBlocking(self, deduper): #, step_size):
        logging.info('Start blocking...')
        """ To run blocking on such a large set of data, we create a separate table
         that contains blocking keys and record ids """
        logging.info('creating blocking_map table in database')
        self.dictCursor.execute("DROP TABLE IF EXISTS blocking_map")
        q = "CREATE TABLE blocking_map (block_key VARCHAR(200), %s INTEGER) " \
                  "CHARACTER SET utf8 COLLATE %s" % (self.tablePK, self.tableCollate)
        self.dictCursor.execute(q)

        """ If dedupe learned a Index Predicate, we have to take a pass
         through the data and create indices. """
        if self.isActiveLearning :
            for field in deduper.blocker.index_fields :
                q = "SELECT DISTINCT %s FROM %s" % (field, self.tableName)
                self.tupleCursor.execute(q)

                field_data = (row[0] for row in self.tupleCursor)
                deduper.blocker.index(field_data, field)

        """ Now we are ready to write our blocking map table by creating a
         generator that yields unique `(block_key, self.tablePK)` tuples. """
        logging.info('generating blocking map')
        self.dictCursor.execute("SELECT DISTINCT %s FROM %s" % (self.columns, self.tableName)) # DELETE DISTINCT!
        self.recordsCount = self.dictCursor.rowcount

        full_data = ((row[self.tablePK], row) for row in self.dictCursor)
        b_data = deduper.blocker(full_data)

        """ MySQL has a hard limit on the size of a data object that can be
         passed to it.  To get around this, we chunk the blocked data in
         to groups of 30,000 blocks """
        pool = dedupe.backport.Pool(processes = self.processes)
        done = False

        logging.info('writing of blocking map...')
        while not done :
            chunks = (list(itertools.islice(b_data, step)) for step in [self.step_size]*100)
           
            results = []

            for chunk in chunks :
                results.append(pool.apply_async(self.dbWriter ("INSERT INTO blocking_map VALUES (%s, %s)", chunk)))

            for r in results :
                r.wait()

            if len(chunk) < self.step_size :
                done = True

        pool.close()

        # Free up memory by removing indices we don't need anymore
        deduper.blocker.resetIndices()

        """ Remove blocks that contain only one record, sort by block key and
         data, key and index blocking map.

         These steps, particularly the sorting will let us quickly create
         blocks of data for comparison """
        logging.info('prepare blocking table. this will probably take a while ...')
        logging.info('indexing block_key')
        self.dictCursor.execute("ALTER TABLE blocking_map ADD UNIQUE INDEX (block_key, %s)" % self.tablePK)

        logging.info('drop supporting tables')
        self.dictCursor.execute("DROP TABLE IF EXISTS plural_key")
        self.dictCursor.execute("DROP TABLE IF EXISTS plural_block")
        self.dictCursor.execute("DROP TABLE IF EXISTS covered_blocks")
        self.dictCursor.execute("DROP TABLE IF EXISTS smaller_coverage")

        """ Many block_keys will only form blocks that contain a single
         record. Since there are no comparisons possible within such a
         singleton block we can ignore them. """
        logging.info('calculating plural_key')
        q = "CREATE TABLE plural_key " \
                  "(block_key VARCHAR(200), " \
                  " block_id INTEGER UNSIGNED AUTO_INCREMENT, " \
                  " PRIMARY KEY (block_id)) " \
                  "(SELECT block_key FROM " \
                  " (SELECT block_key, " \
                  "  GROUP_CONCAT(%(id)s ORDER BY %(id)s) AS block " \
                  "  FROM blocking_map " \
                  "  GROUP BY block_key HAVING COUNT(*) > 1) AS blocks " \
                  " GROUP BY block)" % {'id': self.tablePK}
        self.dictCursor.execute(q)

        logging.info('creating block_key index')
        q = "CREATE UNIQUE INDEX block_key_idx ON plural_key (block_key)"
        self.dictCursor.execute(q)

        logging.info('calculating plural_block')
        q = "CREATE TABLE plural_block (SELECT block_id, %s " \
                  " FROM blocking_map INNER JOIN plural_key " \
                  " USING (block_key))" % self.tablePK
        self.dictCursor.execute(q)

        logging.info('adding "%s" index and sorting index' % self.tablePK)
        q = "ALTER TABLE plural_block ADD INDEX (%(id)s), " \
                  "ADD UNIQUE INDEX (block_id, %(id)s)" % {'id': self.tablePK}
        self.dictCursor.execute(q)

        """ To use Kolb, et.al's Redundant Free Comparison scheme, we need to
         keep track of all the block_ids that are associated with a
         particular data records. We'll use MySQL's GROUP_CONCAT function to
         do this. This function will truncate very long lists of associated
         ids, so we'll also increase the maximum string length to try to avoid this. """
        self.dictCursor.execute("SET group_concat_max_len = 2048")

        logging.info('creating covered_blocks')
        q = "CREATE TABLE covered_blocks (SELECT %(id)s, GROUP_CONCAT(block_id ORDER BY block_id) AS sorted_ids " \
                  " FROM plural_block GROUP BY %(id)s)" % {'id': self.tablePK}
        self.dictCursor.execute(q)
        q = "CREATE UNIQUE INDEX %(id)sx ON covered_blocks (%(id)s)" % {'id': self.tablePK}
        self.dictCursor.execute(q)

        """ In particular, for every block of records, we need to keep
         track of a data records' associated block_ids that are SMALLER than
         the current block's id. Because we ordered the ids when we did the
         GROUP_CONCAT we can achieve this by using some string hacks. """
        logging.info('creating smaller_coverage')
        q = "CREATE TABLE smaller_coverage (SELECT %(id)s, block_id, " \
                  " TRIM(',' FROM SUBSTRING_INDEX(sorted_ids, block_id, 1)) AS smaller_ids " \
                  " FROM plural_block INNER JOIN covered_blocks USING (%(id)s))" % {'id': self.tablePK}
                  # " IF(SUBSTRING_INDEX(sorted_ids, block_id, 1) > 0, TRIM(',' FROM SUBSTRING_INDEX(sorted_ids, block_id, 1)) , block_id) AS smaller_ids " \
        self.dictCursor.execute(q)
        self.con.commit()
        
        logging.info('Blocking is finished')

    def generateGroups(self, result_set) :
        lset = set

        block_id = None
        records = []
        i = 0
        for row in result_set :
            if row['block_id'] != block_id :
                if records :
                    yield records

                block_id = row['block_id']
                records = []
                i += 1

                if i % 1000 == 0 :
                    logging.info('%s, blocks' % i)
                    # print(time.time() - start_time, "seconds")

            smaller_ids = row['smaller_ids']

            if smaller_ids: # and ',' in smaller_ids : # DELETE THIS
                smaller_ids = lset(smaller_ids.split(','))
            else :
                smaller_ids = lset([])

            records.append((row[self.tablePK], row, smaller_ids))
        if records :
            yield records

    def startClustering(self, deduper):
    # Clustering
        logging.info('create dictoninary of processed records')
        q = "SELECT %s, block_id, smaller_ids FROM smaller_coverage INNER JOIN %s " \
                  "USING (%s) ORDER BY (block_id)" % (self.columns, self.tableName, self.tablePK)
        self.dictCursor.execute(q)
        logging.info('start clustering...')

        return deduper.matchBlocks(self.generateGroups(self.dictCursor), threshold=self.threshold) #0.5)

    def saveResults(self, clustered_dupes):
        """
        We now have a sequence of tuples of data ids that dedupe believes
        all refer to the same entity. We write this out onto an entity map
        table
        """
        self.dictCursor.execute("DROP TABLE IF EXISTS %s_entity_map" % self.tableName)

        logging.info('creating %s_entity_map database' % self.tableName)
        q = "CREATE TABLE %(tName)s_entity_map (%(id)s INTEGER, canon_id INTEGER, " \
                  " cluster_score FLOAT, PRIMARY KEY(%(id)s))" % {'tName' : self.tableName, 'id': self.tablePK}
        self.dictCursor.execute(q)

        for cluster, scores in clustered_dupes :
            cluster_id = cluster[0]
            for id, score in zip(cluster, scores) :
                self.dictCursor.execute('INSERT INTO %s_entity_map VALUES (%s, %s, %s)' % (self.tableName, id, cluster_id, score))

        self.con.commit()

        self.dictCursor.execute("CREATE INDEX head_index ON %s_entity_map (canon_id)" % self.tableName)
        self.con.commit()
        
        logging.info('results are saved')

    def closeAllConnections(self):
        if hasattr(self, 'con'):
            self.con.close()

        if hasattr(self, 'con2'):
            self.con2.close()

    def resetAnalyzer(self):
        self.dupesCount = 0
        self.recordsCount = 0
        self.isActiveLearning = False
        self.closeAllConnections()

    def getParameters(self, dataModelFile):
        """ Define the Performance parameters and Database source table. """
        if os.path.exists(dataModelFile):
            logging.info('reading data structure from %s' % dataModelFile)
            with open(dataModelFile) as df :
                data = json.load(df)

                params = data['performance']
                self.cpu = params['cores']
                self.processes = params['threads']
                self.step_size = params['step_size']

                params = data['source_db']
                self.tableCollate = params['collate']
                self.tableName = params['tab_name']
                self.tablePK = params['tab_id']
                self.columns = params['tab_columns']
                
                params = data['training']
                self.isActiveLearning = params['isActive']
                self.threshold = params['threshold']        # Lowering the number will increase recall, raising it will increase precision
                return True
        else: 
            logging.warning('WARNING! Could not read performance parameters and database source table from %s' % dataModelFile)
            return False

def start(dataModelFile = MODEL_FILE):
    global analyzer
    analyzer = analyzer(dataModelFile)       # initiate analyser
    analyzer.startAnalyze(analyzer.isActiveLearning)
    print('Found %s dublicate pairs' % analyzer.dupesCount)

# start with forcing training 
def startWithTraining(dataModelFile = MODEL_FILE):
    global analyzer

    logging.info('Running in training mode')
    analyzer = analyzer(dataModelFile)       # initiate analyser
    analyzer.startAnalyze(True)
    print('Found %s dublicate pairs' % analyzer.dupesCount)

def reset():
    analyzer.resetAnalyzer()

# run by default
if __name__ == "__main__":
    start('data_model.json')
    # startWithTraining('data_model.json')
