# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_get_corpus_from_db.py
# Description:
#

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-8-15 20:21:56
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import MySQLdb
import time
import logging
################################### PART2 CLASS && FUNCTION ###########################
class class_get_corpus_from_db(object):
    def __init__(self, database_name):
        """ Initialize a entry of class.
        Args:
            database_name   (str): a string stored the database's name.
            table_name      (str): a string stored the table's name prepared to be created.
        Returns:
            None
        """
        self.start = time.clock()
        logging.basicConfig(level = logging.DEBUG,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S:%SS',
                  filename = '../main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        logging.info("START at " + time.strftime('%Y-%m-%d %X', time.localtime()))

        try:
            self.con = MySQLdb.connect(host = "localhost", user = "root", passwd = "931209", db = database_name, charset = "utf8")
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))



    def __del__(self):
        """ Delete a entry of class.
        Args:
            None
        Returns:
            None
        """
        self.con.close()
        self.stop = time.clock()
        logging.info("Quit database successfully.")
        logging.info("The class run time is : %.03f seconds" % (self.stop - self.start))
        logging.info("END at:" + time.strftime('%Y-%m-%d %X', time.localtime()))



    def get_essay_list_from_db(self, database_name, table_name):
        """ Get essays from table(table_name) of database(database_name).
        Args:
            database_name   (str): a string stored the database's name.
            table_name      (str): a string stored the essays data table's name.
        Returns:
            essay_list      (list): a list contains essay string(each element in
         list is a string).
        """
        pass
################################### PART3 CLASS TEST ##################################
# Initialize parameters
database_name = "wordsDB"
table_name = "ngram_word_table"

GetCorpus = class_get_corpus_from_db(database_name = database_name)