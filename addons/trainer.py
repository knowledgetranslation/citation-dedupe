#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import logging

import mysql.connector
import json
import dedupe

PATH = os.path.dirname(__file__)
SETTINGS_FILE = PATH +'/'+ 'dedupe_settings'
TRAINING_FILE = PATH +'/'+ 'dedupe_training.json'

ROOTPATH, _ = os.path.split(PATH)
MODEL_FILE = ROOTPATH +'/'+ 'data_model.json'
MYSQL_CONFIG = ROOTPATH +'/'+ 'db.cnf'

class trainer(object):
    def __init__(self, modelFile = MODEL_FILE, tableName =''):
        self.tableName = tableName
        
        if not self.getDataModel(modelFile):
            logging.warning('WARNING! Please, choose other Data Model file against %s' % modelFile)
            self.resetTraining()
            # sys.exit(0)
            quit
            return None
            
        self.recordsCount = 0
        self.connection = mysql.connector.connect(option_files = MYSQL_CONFIG)
        self.cursor = self.connection.cursor(dictionary = True, buffered = False)
        self.cursor.execute("SET net_write_timeout = 3600")
        
    def startTraining(self):
        logging.info('start training...')
        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(self.fields, num_cores = self.cpu)

        # We will sample pairs from the entire data table for training
        logging.info('loading sample data from table "%s"' % self.tableName)
        self.cursor.execute('SELECT %s FROM %s' % (self.columns, self.tableName))

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
        """
        'ppc' (0.1) - Limits the Proportion of Pairs Covered that we allow a predicate to cover. 
        If a predicate puts together a fraction of possible pairs greater than the ppc, 
        that predicate will be removed from consideration. As the size of the data increases, 
        the user will generally want to reduce ppc.
        'ppc' should be a value between 0.0 and 1.0.

        'uncovered_dupes' (1) – The number of true dupes pairs in our training data that we can 
        accept will not be put into any block. If true duplicates are never in the same block, 
        we will never compare them, and may never declare them to be duplicates.

        However, requiring that we cover every single true dupe pair may mean that we have to use 
        blocks that put together many, many distinct pairs that we’ll have to expensively, compare as well.

        index_predicates (True) – Should dedupe consider predicates that rely upon indexing the data. 
        Index predicates can be slower and take susbstantial memory.
        """
        logging.info('saving training data to %s' % TRAINING_FILE)

        try:
            deduper.train()
        except Exception as e:
            err = ValueError
            logging.info(ValueError)
            self.closeAllConnections()
            return None
        else:
            # deduper.train(ppc = self.accuracy)
            # deduper.train(ppc = self.accuracy, uncovered_dupes = self.uncovered)

            # When finished, save our labelled, training pairs to disk
            self.saveSettingFiles(deduper)
            
            # We can now remove some of the memory hobbing objects we used for training
            logging.info('Cleaning unused data')
            deduper.cleanupTraining()

            logging.info('Training was finished')
            self.closeAllConnections()
            return deduper

    def startActiveLearning(self, deduper):
        logging.info('Starting active labelling,  use "y", "n" and "u" keys to flag duplicates press "f" when you are finished')
        """ Starts the training loop. Dedupe will find the next pair of records
        it is least certain about and ask you to label them as duplicates or not. """
        dedupe.convenience.consoleLabel(deduper)
        
    def saveSettingFiles(self, deduper):
        logging.info('Saving training data to %s' % TRAINING_FILE)
        with open(TRAINING_FILE, 'w') as tf:
            deduper.writeTraining(tf)

        logging.info('Saving setting data to %s' % SETTINGS_FILE)
        with open(SETTINGS_FILE, 'wb') as sf:
            deduper.writeSettings(sf)

    def getDataModel(self, dataModelFile):
        """ Define the fields from source DB which Dedupe will pay attention to.
        Limit parameters for improving Performance. And training parameters."""
        if os.path.exists(dataModelFile):
            logging.info('reading data structure from %s' % dataModelFile)
            with open(dataModelFile) as df :
                data = json.load(df)
                self.fields = data['fields']

                params = data['performance']
                self.cpu = params['cores']
                self.processes = params['threads']
                self.step_size = params['step_size']
                
                
                params = data['training']
                self.samples_size = params['samples']
                self.accuracy = params['accuracy']
                self.uncovered = params['uncovered']

                params = data['source_db']
                self.tableCollate = params['collate']
                self.tablePK = params['tab_id']
                self.columns = params['tab_columns']
                # self.tableName = params['tab_name']
                
                if self.tableName == '':            # added for web API v1
                    self.tableName = params['tab_name']
                return True
        else: 
            logging.warning('WARNING! Could not read data structure from %s' % dataModelFile)
            return False

    def closeAllConnections(self):
        if hasattr(self, 'connection'):
            self.connection.close()

    def resetTraining(self):
        self.recordsCount = 0
        self.closeAllConnections()

def start(dataModelFile = MODEL_FILE):
    global trainer
    trainer = trainer(dataModelFile)
    trainer.startTraining()

def reset():
    trainer.resetTraining()
    
if __name__ == "__main__":
    start('data_model.json')