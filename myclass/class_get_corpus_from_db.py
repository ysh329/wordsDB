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
                  datefmt = '%y-%m-%d %H:%M:%S',
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
        essay_list = []
        cursor = self.con.cursor()
        try:
            sql = """SELECT date, title, content FROM %s.%s""" % (database_name, table_name)
            cursor.execute(sql)
            essay_tuple = cursor.fetchall()

            if len(essay_tuple) > 1:
                for idx in xrange(len(essay_tuple)):
                    date = essay_tuple[idx][0]
                    title = essay_tuple[idx][1]
                    content = essay_tuple[idx][2]
                    try:
                        if type(title) != unicode: title = unicode(title, "utf8")
                        if type(content) != unicode: content = unicode(content, "utf8")
                        essay_list.append([date, title, content])
                    except:
                        logging.error("Transform encoding to unicode failed.")
                        logging.error("essay_tuple[0][0]:%s" ,essay_tuple[0][0])
                        logging.error("essay_tuple[0][1]:%s" ,essay_tuple[0][1])
                        logging.error("essay_tuple[0][2]:%s" ,essay_tuple[0][2])
                        continue
        except MySQLdb.Error, e:
            logging.error("Failed in selecting essays'title and content from table %s database %s." % (table_name, database_name))
            logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
            return essay_list

        logging.info("essay_tuple[0][0]:%s" % (essay_tuple[0][0]))
        logging.info("essay_tuple[0][1]:%s" % (essay_tuple[0][1]))
        logging.info("essay_tuple[0][2]:%s" % (essay_tuple[0][2]))
        logging.info("Get essay list successfully.")
        logging.info("Get essay record %d." % (len(essay_list)))

        essay_str_list = map(lambda essay:essay[1] + essay[2], essay_list)
        logging.info("len(essay_str_list)):%d", len(essay_str_list))
        logging.info("essay_str_list[0]:%s" % essay_str_list[0])
        logging.info("essay_str_list[1]:%s" % essay_str_list[1])
        logging.info("type(essay_str_list[0]):%s" % type(essay_str_list[0]))

        date_list = map(lambda essay: essay[0], essay_list)
        logging.info("len(date_list):%s" % len(date_list))
        logging.info("date_list[0]:%s" % date_list[0])
        logging.info("type(date_list[0]):%s" % type(date_list[0]))
        #return essay_str_list, date_list
        return essay_list

################################### PART3 CLASS TEST ##################################
"""
# Initialize parameters
word_database_name = "wordsDB"
word_table_name = "ngram_word_table"
essay_database_name = "essayDB"
essay_table_name = "securities_newspaper_shzqb_table"

# table1: securities_newspaper_shzqb_table
# table2: securities_newspaper_zgzqb_table
# table3: securities_newspaper_zqrb_table
# table4: securities_newspaper_zqsb_table

GetCorpus = class_get_corpus_from_db(database_name = word_database_name)
GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay_table_name)
"""