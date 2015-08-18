# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_get_ngram_2_db.py
# Description:
#

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-8-17 22:17:26
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import MySQLdb
import time
import logging
################################### PART2 CLASS && FUNCTION ###########################
class class_get_ngram_2_db(object):
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



    def get_one_bi_tri_gram(raw_string):
        """ Create table(table_name) of database(database_name), which is used for storing
         words.
        Args:
            database_name   (str): a string stored the database's name.
            table_name      (str): a string stored the table's name prepared to be created.
        Returns:
            None
        """
        one_gram_list = []
        bi_gram_list = []
        tri_gram_list = []

        for idx in xrange(len(raw_string)):
            # one-gram
            one_gram = raw_string[idx]
            one_gram_list.append(one_gram)
            # bi-gram
            if len(raw_string) > idx + 1:
                bi_gram = raw_string[idx:idx+2]
                bi_gram_list.append(bi_gram)
            else: bi_gram = None
            # tri-gram
            if len(raw_string) > idx + 2:
                tri_gram = raw_string[idx:idx+3]
                tri_gram_list.append(tri_gram)
            else: tri_gram = None
        return (one_gram_list, bi_gram_list, tri_gram_list)



success_counter = 0
failure_counter = 0
def insert_ngram_2_db(word, showtimes, database_name, table_name):
    global success_counter, failure_counter

    try:
        con = MySQLdb.connect(host = "localhost", user = "root", passwd = "931209", db = database_name, charset = "utf8")
        logging.info("Success in connecting MySQL.")
    except MySQLdb.Error, e:
        logging.error("Fail in connecting MySQL.")
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))

    cursor = con.cursor()
    try:
        cursor.execute("""SELECT id FROM %s.%s WHERE word='%s'"""\
                       % (database_name, table_name, word)
                       )
        id_tuple = cursor.fetchone()
        logging.info("id_tuple:%s" % id_tuple)
        if id_tuple == None:
            # dont exist word
            try:
                cursor.execute("""INSERT INTO %s.%s
                               (word, pinyin, showtimes, weight, cixing, type1, type2, source, gram, meaning)
                               VALUES('%s', '', '%s', 0.0, 'cx', 't1', 't2', 'stock-newspaper-essence', '%s', 'ex')"""\
                               % (database_name, table_name, word, showtimes, len(word))\
                              )

                con.commit()
                success_counter += 1
            except MySQLdb.Error, e:
                failure_counter += 1
                con.rollback()
                logging.error("Failed in inserting %s gram word %s, which is existed."\
                              % (len(word), word))
                logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
        else: # exited word
            id = id_tuple[0]
            logging.info("id:%s" % id)
            try:
                cursor.execute("""UPDATE %s.%s
                               SET showtimes=showtimes+'%s',
                                   gram='%s'
                               WHERE id='%s'"""\
                               % (database_name, table_name, showtimes, len(word), id)\
                              )
                con.commit()
                success_counter += 1
            except MySQLdb.Error, e:
                failure_counter += 1
                con.rollback()
                logging.error("Failed in updating %s gram word %s, which is existed."\
                              % (len(word), word))
                logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    except MySQLdb.Error, e:
        failure_counter += 1
        con.rollback()
        logging.error("Fail in selecting %s gram word %s in table %s of database %s."\
                      % (len(word), word, table_name, database_name))
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    finally:
        con.close()
    return None



def computation_corpus_scale(self, database_name, table_name):
    pass

################################### PART3 CLASS TEST ##################################