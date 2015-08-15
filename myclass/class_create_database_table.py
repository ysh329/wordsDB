# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_database_table.py
# Description:
#             Create database named 'wordsDB', which used to make statistic about n-gram
#      word using stock newspapers essays.
#      which contains 10 fields(id, word, pinyin, showtimes, weight, meaning, cixing,
#      type1, type2, source).

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-8-15 17:43:04
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import MySQLdb
import time
import logging
################################### PART2 CLASS && FUNCTION ###########################
class class_create_database_table(object):
    def __init__(self):
        """ Initialize a entry of class.
        Args:
            database_name (str): input database name
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
            self.con = MySQLdb.connect(host = "localhost", user = "root", passwd = "931209", charset = "utf8")
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



    def create_database(self, database_name):
        """ Create database named database_name('wordsDB'), which is used for storing words.
        Args:
            database_name (str): a string stored the database's name prepared to be created.
        Returns:
            None
        """
        cursor = self.con.cursor()
        sql_list = ['SET NAMES UTF8', 'SELECT VERSION()', 'CREATE DATABASE %s' % database_name]
        try:
            map(lambda sql: cursor.execute(sql), sql_list)
            self.con.commit()
            logging.info("Successfully create database %s in MySQL." % database_name)
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in creating database(executing sqls:%s)." % ';'.join(sql_list))
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))



    def create_table(self, database_name, table_name):
        """ Create table(table_name) of database(database_name), which is used for storing
         words.
        Args:
            database_name   (str): a string stored the database's name.
            table_name      (str): a string stored the table's name prepared to be created.
        Returns:
            None
        """
        cursor = self.con.cursor()

        sqls = ["USE %s" % database_name, "SET NAMES UTF8"]
        sqls.append("ALTER DATABASE %s DEFAULT CHARACTER SET 'utf8'" % database_name)
        sqls.append("""CREATE TABLE IF NOT EXISTS %s
                                (
                                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                word VARCHAR(100) NOT NULL,
                                pinyin VARCHAR(100) NOT NULL,
                                showtimes INT(11) NOT NULL DEFAULT 0,
                                weight FLOAT(11) NOT NULL DEFAULT 0.0,
                                cixing VARCHAR(10) NOT NULL,
                                type1 VARCHAR(30) NOT NULL,
                                type2 VARCHAR(30) NOT NULL,
                                source VARCHAR(50) NOT NULL,
                                gram int(11),
                                meaning TEXT NOT NULL,
                                UNIQUE (word)
                                )""" % table_name)
        sqls.append("CREATE INDEX id_idx ON %s(id)" % table_name)
        try:
            map(lambda sql:cursor.execute(sql), sqls)
            self.con.commit()
            logging.info("Successfully create table %s in MySQL." % table_name)
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in creating table %s of database %s." % (table_name, database_name))
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))

################################### PART3 CLASS TEST ##################################
#'''
# Initialize parameters
database_name = "wordsDB"
table_name = "ngram_word_table"

CreateDBandTable = class_create_database_table()
CreateDBandTable.create_database(database_name = database_name)
CreateDBandTable.create_table(database_name = database_name, table_name = table_name)
#'''