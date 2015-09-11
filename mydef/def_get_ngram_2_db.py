# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: def_get_ngram_2_db.py
# Description:
#

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-8-17 22:17:26
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import MySQLdb
import logging
################################### PART2 CLASS && FUNCTION ###########################
def get_one_bi_tri_gram(raw_string):
    """ Get onegram, bigram, trigram from raw_string and
     return.
    Args:
        raw_string   (str): constitution.txt string stored the text
    Returns:
        (one_gram_list, bi_gram_list, tri_gram_list) (tuple):
            each element in tuple is constitution.txt list of onegram or
            bigram or trigram.
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
        # tri-gram
        if len(raw_string) > idx + 2:
            tri_gram = raw_string[idx:idx+3]
            tri_gram_list.append(tri_gram)
    return (one_gram_list, bi_gram_list, tri_gram_list)



def insert_ngram_2_db(word, showtimes, database_name, table_name):
    """ Insert ngram(word) and its show times(showtimes) in corpus to
     table(table_name) of database(database_name).
    Args:
         word            (str): ngram word
         showtimes       (int): this ngram word's show times in corpus
         database_name   (str): name of preparing inserted database
         table_name      (str): name of preparing inserted table
    Returns:
        None
    """
    try:
        con = MySQLdb.connect(host = "localhost", user = "root", passwd = "931209", db = database_name, charset = "utf8")
        #logging.info("Success in connecting MySQL.")
    except MySQLdb.Error, e:
        logging.error("Fail in connecting MySQL.")
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    cursor = con.cursor()
    try:
        cursor.execute("""SELECT id FROM %s.%s WHERE word='%s'"""\
                       % (database_name, table_name, word)
                       )
        id_tuple = cursor.fetchone()
        if id_tuple == None: # not existed word
            try:
                cursor.execute("""INSERT INTO %s.%s
                               (word, pinyin, showtimes, weight, cixing, type1, type2, source, gram, meaning)
                               VALUES('%s', '', '%s', 0.0, 'cx', 't1', 't2', 'stock-newspaper-essence', '%s', 'ex')"""\
                               % (database_name, table_name, word, showtimes, len(word))\
                              )

                con.commit()
            except MySQLdb.Error, e:
                con.rollback()
                logging.error("Failed in inserting %s gram word %s, which is existed."\
                              % (len(word), word))
                logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
        else: # exited word
            id = id_tuple[0]
            try:
                cursor.execute("""UPDATE %s.%s
                               SET showtimes=showtimes+'%s',
                                   gram='%s'
                               WHERE id='%s'"""\
                               % (database_name, table_name, showtimes, len(word), id)\
                              )
                con.commit()
            except MySQLdb.Error, e:
                con.rollback()
                logging.error("Failed in updating %s gram word %s, which is existed."\
                              % (len(word), word))
                logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    except MySQLdb.Error, e:
        con.rollback()
        logging.error("Fail in selecting %s gram word %s in table %s of database %s."\
                      % (len(word), word, table_name, database_name))
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    finally:
        con.close()
    return None



def computation_corpus_scale_and_weight_2_db(database_name, table_name):
    """ Compute the scale of corpus. Different ngram word, its corpus
     scale is different, such as bigram word's corpus scale need to
     compute the quantity of bigram words.
    Args:
         database_name   (str): name of preparing updated database
         table_name      (str): name of preparing updated table
    Returns:
        None
    """
    try:
        con = MySQLdb.connect(host = "localhost",\
                              user = "root",\
                              passwd = "931209",\
                              db = database_name,\
                              charset = "utf8")
        logging.info("Success in connecting MySQL.")
    except MySQLdb.Error, e:
        logging.error("Fail in connecting MySQL.")
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    cursor = con.cursor()
    try:
        sql_list = []
        sql_list.append("SET @onegram_num = (SELECT SUM(showtimes) FROM %s.%s WHERE gram = 1)" % (database_name, table_name))
        sql_list.append("SET @bigram_num = (SELECT SUM(showtimes) FROM %s.%s WHERE gram = 2)" % (database_name, table_name))
        sql_list.append("SET @trigram_num = (SELECT SUM(showtimes) FROM %s.%s WHERE gram = 3)" % (database_name, table_name))
        sql_list.append("UPDATE %s.%s SET corpus_scale = @onegram_num WHERE gram = 1" % (database_name, table_name))
        sql_list.append("UPDATE %s.%s SET corpus_scale = @bigram_num WHERE gram = 2" % (database_name, table_name))
        sql_list.append("UPDATE %s.%s SET corpus_scale = @trigram_num WHERE gram = 3" % (database_name, table_name))
        sql_list.append("UPDATE %s.%s SET weight = (showtimes / corpus_scale)" % (database_name, table_name))
        map(lambda sql: cursor.execute(sql), sql_list)
        con.commit()
        logging.info("Success in updating corpus scale and weight of words.")
    except MySQLdb.Error, e:
        con.rollback()
        logging.error("Fail in selecting gram word in table %s of database %s."\
                      % (table_name, database_name))
        logging.error("MySQL Error %d: %s." % (e.args[0], e.args[1]))
    finally:
        con.close()
    return None
################################### PART3 CLASS TEST ##################################