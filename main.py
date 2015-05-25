#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
import os, sys
import argparse # optparse
import time
import logging

sys.path.append(os.getcwd()) # Add current folder as PATH to module
import analyzer
# from addons 
# import addons.trainer

# PATH = os.path.abspath('.') +'/'
PATH = os.path.dirname(__file__) +'/'
MODEL_FILE = PATH + 'data_model.json'

# MYSQL_CONFIG = PATH + 'db.cnf'
# SETTINGS_FILE = PATH + 'dedupe_settings'
# TRAINING_FILE = PATH + 'dedupe_training.json'

def options():
    parser = argparse.ArgumentParser(description='Dedupe analyses Data from Database for identify duplicate records. It based on Dedupe library for Python.'+
   ' It can be used in Training mode for machinery learning(creating clusters with weight for each similar records pairs). '+
   ' For setup it uses Data Model and loads it from file in .JSON format.'+ 
   ' As output it generates mapping table and stores there all duplicate records.')
    parser.add_argument('-t', '--training', dest='forceTraining', action='store_true',
                    help='To start Analyzer in training mode use "-t" or "--training"'
                    )
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify twice for DEBUG)'
                    )
    return parser.parse_args()

def initLogging(log_level = logging.WARNING):
    logging.getLogger().setLevel(log_level)

def main():
    """
     Uses Python logging to show or suppress verbose output.
     To enable verbose output, run `python main.py -v (specify twice for DEBUG)`
     To force training mode, run `python main.py -t`
    """
    log_level = logging.WARNING
    start_time = time.time()    # set start time

    opt = options()
    if opt :
        if opt.verbose :
            if opt.verbose == 1:
                log_level = logging.INFO
            elif opt.verbose >= 2:
                log_level = logging.DEBUG
        initLogging(log_level)              # start logging
        if opt.forceTraining:
            # start analysis in training mode
            analyzer.startWithTraining()    # by Default argument = MODEL_FILE
        else:
            # start analysis
            analyzer.start()                # by Default argument = MODEL_FILE
    else:
        initLogging(log_level)              # start logging
        # start analysis
        analyzer.start()                    # by Default argument = MODEL_FILE

    print('Task completed successful! Ran in', round(time.time() - start_time, 1), 'seconds')

if __name__=='__main__':
    main()