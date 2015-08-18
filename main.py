# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: main.py
# Description:

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-8-15 19:46:52
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
from myclass.class_create_database_table import *
from myclass.class_get_corpus_from_db import *
from pyspark import SparkContext, SparkConf
################################### PART2 MAIN && FUNCTION ############################
def main():

    # Initialize parameters
    database_name = "wordsDB"
    table_name = "ngram_word_table"

    CreateDBandTable = class_create_database_table()
    CreateDBandTable.create_database(database_name = database_name)
    CreateDBandTable.create_table(database_name = database_name, table_name = table_name)


    # Initialize parameters
    essay_database_name = "essayDB"

    # table1: securities_newspaper_shzqb_table
    # table2: securities_newspaper_zgzqb_table
    # table3: securities_newspaper_zqrb_table
    # table4: securities_newspaper_zqsb_table

    GetCorpus = class_get_corpus_from_db()
    essay1_table_name = "securities_newspaper_shzqb_table"
    essay2_table_name = "securities_newspaper_zgzqb_table"
    essay3_table_name = "securities_newspaper_zqrb_table"
    essay4_table_name = "securities_newspaper_zqsb_table"

    essay1_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay1_table_name)
    essay2_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay2_table_name)
    essay3_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay3_table_name)
    essay4_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay4_table_name)

    all_essay_list = []
    all_essay_list.extend(essay1_list)
    all_essay_list.extend(essay2_list)
    all_essay_list.extend(essay3_list)
    all_essay_list.extend(essay4_list)

    print "len(all_essay_list):", len(all_essay_list)
    logging.info("len(all_essay_list):%s" % len(all_essay_list))

    # Initialize parameters
    #sparkHome = "/home/yuens/MySpark/spark-1.4.1-bin-hadoop2.6/"
    appName = "wordsDBApp"
    conf = SparkConf().setAppName(appName).setMaster("local")
    sc = SparkContext(conf = conf)
    essay_tuple_rdd = sc.parallelize(all_essay_list)
    essay_title_gram_rdd = essay_tuple_rdd.map(lambda essay_tuple: one_bi_tri_gram(essay_tuple[1]))
    essay_content_gram_rdd = essay_tuple_rdd.map(lambda essay_tuple: one_bi_tri_gram(essay_tuple[2]))



    sc.stop()

    logging.info("END at:" + time.strftime('%Y-%m-%d %X', time.localtime()))
################################ PART4 EXECUTE ########################################
if __name__ == "__main__":
    main()