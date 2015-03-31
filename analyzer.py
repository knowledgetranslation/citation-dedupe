#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
""" This Module analyses Data from Database for duplicate records.
 It used Training method to create block of "options" for Active learning.
 It loads Data Model from file in JSON format. As output it generates
 mapping table for all duplicate records.

 __Note:__ To create related tables in Database you need to run `/init_db.py` 
 before running this script. """
 
from __future__ import print_function

import os
import sys
import itertools
import time
import logging
import optparse
import locale
import pickle
import multiprocessing

import mysql.connector
import dedupe
import json

# FILES VARS
MYSQL_CONFIG = os.path.abspath('.') +'/'+ 'db.cnf'
MODEL_FILE = os.path.abspath('.') +'/'+ 'data_model.json'

SETTINGS_FILE = os.path.abspath('.') +'/'+ 'dedupe_settings'
TRAINING_FILE = os.path.abspath('.') +'/'+ 'dedupe_training.json'

class trainer(object):
    def __init__(self):
        self.recordsCount = 0

        self.connection = mysql.connector.connect(option_files = MYSQL_CONFIG)
        self.cursor = self.connection.cursor(dictionary = True, buffered = False)
        self.cursor.execute("SET net_write_timeout = 3600")

        self.getDataModel(MODEL_FILE)

    def startActiveLearning(self, deduper):
        logging.info("starting active labelling,  use 'y', 'n' and 'u' keys to flag duplicates press 'f' when you are finished")
        """ Starts the training loop. Dedupe will find the next pair of records
         it is least certain about and ask you to label them as duplicates or not. """
        dedupe.convenience.consoleLabel(deduper)

    def startTraining(self):
        logging.info('start training...')
        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(self.fields, num_cores = self.cpu)

        # We will sample pairs from the entire data table for training
        logging.info("loading samples' data from table '%s'" % self.tableName)
        self.cursor.execute("SELECT %s FROM %s" % (self.columns, self.tableName))

        temp_d = dict((i, row) for i, row in enumerate(self.cursor))
        deduper.sample(temp_d, self.samples_size)
        del temp_d
        logging.info('data dictionary was created successfully')

        """ If we have training data saved from a previous run of dedupe, look for it and load it in.
         __Note:__ if you want to train from scratch, delete the TRAINING_FILE """
        if os.path.exists(TRAINING_FILE):
            logging.info('reading labeled examples from ', TRAINING_FILE)
            with open(TRAINING_FILE) as tf :
                deduper.readTraining(tf)

        self.startActiveLearning(deduper) # Start training loop

        """ Notice our two arguments here
         `ppc` limits the Proportion of Pairs Covered that we allow a
         predicate to cover. If a predicate puts together a fraction of
         possible pairs greater than the ppc, that predicate will be removed
         from consideration. As the size of the data increases, the user
         will generally want to reduce ppc (=0.01).

         `uncovered_dupes`(=5) is the number of true dupes pairs in our training
         data that we are willing to accept will never be put into any
         block. If true duplicates are never in the same block, we will never
         compare them, and may never declare them to be duplicates.

         However, requiring that we cover every single true dupe pair may
         mean that we have to use blocks that put together many, many
         distinct pairs that we'll have to expensively, compare as well. """
        logging.info('saving training data to %s' % TRAINING_FILE)
        deduper.train(ppc = self.accuracy)
        # deduper.train(ppc = self.accuracy, uncovered_dupes = self.uncovered)

        # When finished, save our labelled, training pairs to disk
        self.saveSettingFiles(deduper)

        # We can now remove some of the memory hobbing objects we used for training
        logging.info('cleaning unused data')
        deduper.cleanupTraining()

        logging.info('training was finished')
        return deduper

    def saveSettingFiles(self, deduper):
        logging.info('saving training data to %s' % TRAINING_FILE)
        with open(TRAINING_FILE, 'w') as tf:
            deduper.writeTraining(tf)

        logging.info('saving setting data to %s' % SETTINGS_FILE)
        with open(SETTINGS_FILE, 'wb') as sf:
            deduper.writeSettings(sf)

    def getDataModel(self, data_model_file):
        """ Define the fields which dedupe will pay attention to
         The Pages, Volume, Number, ISBN and ISSN and fields are often missing,
         tell dedupe that. """
        if os.path.exists(data_model_file):
            logging.info('reading data structure from %s' % data_model_file)
            with open(data_model_file) as df :
                data = json.load(df)
                self.fields = data['fields']
                self.step_size = data['step_size']

                self.cpu = data['cores']
                self.processes = data['threads']

                self.samples_size = data['samples']
                self.accuracy = data['accuracy']
                self.uncovered = data['uncovered']

                params = data['db_params']
                self.tableCollate = params['collate']
                self.tableName = params['tab_name']
                self.tablePK = params['tab_id']
                self.columns = params['tab_columns']
        else: 
            logging.info("WARNING! could not read data structure from %s" % data_model_file)
            sys.exit(0)

    def closeAllConnections(self):
        self.connection.close()

    def resetTraining(self):
        self.recordsCount = 0

class analyzer(object):
    def __init__(self):
        self.dupesCount = 0
        self.recordsCount = 0
        self.isActiveLearning = False

        self.con = self.connectDB(MYSQL_CONFIG)
        self.con2 = self.connectDB(MYSQL_CONFIG)
        
        self.dictCursor = self.con.cursor(dictionary = True, buffered = False)
        self.tupleCursor = self.con2.cursor(named_tuple = True, buffered = False)

        self.dictCursor.execute("SET net_write_timeout = 3600")
        self.tupleCursor.execute("SET net_write_timeout = 3600")
        self.deduper = ()

    def startAnalyze(self):
        if self.isActiveLearning:
            self.deduper = trainer.startTraining()
            self.processes = trainer.processes
            self.step_size = trainer.step_size
            
            self.tableName = trainer.tableName
            self.tablePK = trainer.tablePK
            self.columns = trainer.columns
            self.tableCollate = trainer.tableCollate

        else:
            logging.info('reading trainnig settings from %s' % SETTINGS_FILE)
            if os.path.exists(SETTINGS_FILE):
                self.getParameters(MODEL_FILE) # Setup CONSTANTS
                with open(SETTINGS_FILE, 'rb') as sf : 
                    self.deduper = dedupe.StaticDedupe(sf, num_cores = self.cpu)
            else:
                logging.info('WARNING! could not find file with trainnig settings, please teach system first')
                sys.exit(0)

        self.startBlocking(self.deduper) #, self.step_size)
        clustered_dupes = self.startClustering(self.deduper)
        logging.info('clustering is finished')

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
        logging.info('start blocking...')
        """ To run blocking on such a large set of data, we create a separate table
         that contains blocking keys and record ids """
        logging.info('creating blocking_map table in database')
        self.dictCursor.execute("DROP TABLE IF EXISTS blocking_map")
        q = "CREATE TABLE blocking_map (block_key VARCHAR(200), %s INTEGER) " \
                  "CHARACTER SET utf8 COLLATE %s" % (self.tablePK, self.tableCollate)
        self.dictCursor.execute(q) # self.dictCursor.execute(q)

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
        
        self.dictCursor.execute("SELECT %s FROM %s" % (self.columns, self.tableName))
        self.recordsCount = self.dictCursor.rowcount # c.rowcount

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
                  "  GROUP_CONCAT(%(tab_id)s ORDER BY %(tab_id)s) AS block " \
                  "  FROM blocking_map " \
                  "  GROUP BY block_key HAVING COUNT(*) > 1) AS blocks " \
                  " GROUP BY block)" % {'tab_id': self.tablePK}
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
        self.dictCursor.execute(q)
        self.con.commit()
        
        logging.info('blocking is finished')

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
                    print(i, "blocks")
                    print(time.time() - start_time, "seconds")

            smaller_ids = row['smaller_ids']

            if smaller_ids :
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

        return deduper.matchBlocks(self.generateGroups(self.dictCursor), threshold=0.5)

    def saveResults(self, clustered_dupes):
        """
        We now have a sequence of tuples of data ids that dedupe believes
        all refer to the same entity. We write this out onto an entity map
        table
        """
        self.dictCursor.execute("DROP TABLE IF EXISTS entity_map")

        logging.info('creating entity_map database')
        q = "CREATE TABLE entity_map (%(id)s INTEGER, canon_id INTEGER, " \
                  " cluster_score FLOAT, PRIMARY KEY(%(id)s))" % {'id': self.tablePK}
        self.dictCursor.execute(q)

        for cluster, scores in clustered_dupes :
            cluster_id = cluster[0]
            for id, score in zip(cluster, scores) :
                self.dictCursor.execute('INSERT INTO entity_map VALUES (%s, %s, %s)' % (id, cluster_id, score))

        self.con.commit()

        self.dictCursor.execute("CREATE INDEX head_index ON entity_map (canon_id)")
        self.con.commit()
        
        logging.info('results are saved')

    def closeAllConnections(self):
        self.con.close()
        self.con2.close()

    def resetAnalysis(self):
        self.dupesCount = 0
        self.recordsCount = 0
        self.isActiveLearning = False

    def getParameters(self, data_model_file):
        """ Define the Performance parameters and Database source table. """
        if os.path.exists(data_model_file):
            logging.info('reading data structure from %s' % data_model_file)
            with open(data_model_file) as df :
                data = json.load(df)

                self.cpu = data['cores']
                self.processes = data['threads']
                self.step_size = data['step_size']

                params = data['db_params']
                self.tableCollate = params['collate']
                self.tableName = params['tab_name']
                self.tablePK = params['tab_id']
                self.columns = params['tab_columns']

        else: 
            logging.info("WARNING! could not read performance parameters and database source table from %s" % data_model_file)
            sys.exit(0)

def start():
    analyzer.startAnalyze()

def reset():
    analyzer.resetAnalysis()

def initLogging():
    """
     Use Python logging to show or suppress verbose output.
     To enable verbose output, run `python analyzer.py -v`
    """
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING 
    if opts.verbose :
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

start_time = time.time()    # set start time
initLogging()               # start logging
analyzer = analyzer()       # initiate analyser

# force training 
analyzer.isActiveLearning = True
if analyzer.isActiveLearning: trainer = trainer()

start()                     # start analysis

print('ran in', time.time() - start_time, 'seconds')
print('Found %s dublicate pairs' % analyzer.dupesCount)