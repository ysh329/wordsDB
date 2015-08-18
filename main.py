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
from myclass.class_get_ngram_2_db import *
from pyspark import SparkContext, SparkConf
from compiler.ast import flatten
################################### PART2 MAIN && FUNCTION ############################
def main():

    # Initialize parameters
    essay_database_name = "essayDB"
    word_database_name = "wordsDB"
    ngram_table_name = "ngram_word_table"
    # table1: securities_newspaper_shzqb_table
    # table2: securities_newspaper_zgzqb_table
    # table3: securities_newspaper_zqrb_table
    # table4: securities_newspaper_zqsb_table
    essay1_table_name = "securities_newspaper_shzqb_table"
    essay2_table_name = "securities_newspaper_zgzqb_table"
    essay3_table_name = "securities_newspaper_zqrb_table"
    essay4_table_name = "securities_newspaper_zqsb_table"
    top_n = int(raw_input("input top n value:"))


    CreateDBandTable = class_create_database_table()
    CreateDBandTable.create_database(database_name = word_database_name)
    CreateDBandTable.create_table(database_name = word_database_name, table_name = ngram_table_name)


    GetCorpus = class_get_corpus_from_db(database_name = essay_database_name)

    essay1_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay1_table_name)
    essay2_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay2_table_name)
    essay3_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay3_table_name)
    essay4_list = GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay4_table_name)
    logging.info("Get essay of 4 newspapers successfully.")
    all_essay_list = []
    all_essay_list.extend(essay1_list)
    all_essay_list.extend(essay2_list)
    all_essay_list.extend(essay3_list)
    all_essay_list.extend(essay4_list)

    logging.info("len(all_essay_list):%s" % len(all_essay_list))

    # Initialize parameters
    #sparkHome = "/home/yuens/MySpark/spark-1.4.1-bin-hadoop2.6/"
    appName = "wordsDBApp"
    conf = SparkConf().setAppName(appName).setMaster("local")
    sc = SparkContext(conf = conf)

    essay_tuple_rdd = sc.parallelize(all_essay_list)
    date_rdd = essay_tuple_rdd.map(lambda essay_tuple: essay_tuple[0])
    title_rdd = essay_tuple_rdd.map(lambda essay_tuple: essay_tuple[1])
    content_rdd = essay_tuple_rdd.map(lambda essay_tuple: essay_tuple[2])

    logging.info("date_rdd.top(1):%s" % date_rdd.top(1))
    logging.info("title_rdd.top(1):%s" % title_rdd.top(1))
    logging.info("content_rdd.top(1):%s" % content_rdd.top(1))
    logging.info("title_rdd.count():%s" % title_rdd.count())
    logging.info("content_rdd.count():%s" % content_rdd.count())

    title_gram_rdd = title_rdd.map(lambda title: get_one_bi_tri_gram(raw_string = title))
    content_gram_rdd = content_rdd.map(lambda content: get_one_bi_tri_gram(raw_string = content))

    # one-gram, bi-gram, tri-gram of title and content
    title_one_gram_rdd = title_gram_rdd.map(lambda title: title[0])
    title_bi_gram_rdd = title_gram_rdd.map(lambda title: title[1])
    title_tri_gram_rdd = title_gram_rdd.map(lambda title: title[2])

    content_one_gram_rdd = content_gram_rdd.map(lambda content: content[0])
    content_bi_gram_rdd = content_gram_rdd.map(lambda content: content[1])
    content_tri_gram_rdd = content_gram_rdd.map(lambda content: content[2])

    # ngram of title and content
    title_one_gram_list = (title_one_gram_rdd\
                           .map(lambda word: map(lambda w: w, word))\
                           .take(title_one_gram_rdd.count()))
    title_bi_gram_list = (title_bi_gram_rdd\
                          .map(lambda word: map(lambda w: w, word))\
                          .take(title_bi_gram_rdd.count()))
    title_tri_gram_list = (title_tri_gram_rdd\
                           .map(lambda word: map(lambda w: w, word))\
                           .take(title_tri_gram_rdd.count()))

    content_one_gram_list = (content_one_gram_rdd\
                             .map(lambda word: map(lambda w: w, word))\
                             .take(content_one_gram_rdd.count()))
    content_bi_gram_list = (content_bi_gram_rdd\
                            .map(lambda word: map(lambda w: w, word))\
                            .take(content_bi_gram_rdd.count()))
    content_tri_gram_list = (content_tri_gram_rdd\
                             .map(lambda word: map(lambda w: w, word))\
                             .take(content_tri_gram_rdd.count()))

    one_gram_list = []
    one_gram_list.extend(title_one_gram_list)
    one_gram_list.extend(content_one_gram_list)
    one_gram_list = flatten(one_gram_list)
    logging.info("len(one_gram_list):%s" % len(one_gram_list))

    bi_gram_list = []
    bi_gram_list.extend(title_bi_gram_list)
    bi_gram_list.extend(content_bi_gram_list)
    bi_gram_list = flatten(bi_gram_list)
    logging.info("len(bi_gram_list):%s" % len(bi_gram_list))

    tri_gram_list = []
    tri_gram_list.extend(title_tri_gram_list)
    tri_gram_list.extend(content_tri_gram_list)
    tri_gram_list = flatten(tri_gram_list)
    logging.info("len(tri_gram_list):%s" % len(tri_gram_list))

    ngram_list = []
    ngram_list.extend(one_gram_list)
    ngram_list.extend(bi_gram_list)
    ngram_list.extend(tri_gram_list)
    logging.info("len(ngram_list):%s" % len(ngram_list))
    logging.info("ngram_list[0]%s:" % ngram_list[0])
    logging.info("ngram_list[:10]:%s" % ngram_list[:10])


    ngram_rdd = sc.parallelize(ngram_list)
    ngram_pair_rdd = (ngram_rdd\
                      .map(lambda word: (word, 1))\
                      .reduceByKey(lambda x, y: x + y)\
                      .map(lambda (word, showtimes): (word, showtimes, len(word))))

    top_bigram_and_trigram_pair_list = (ngram_pair_rdd\
                                        .filter(lambda (word, showtimes, length): length >= 2)\
                                        .takeOrdered(top_n, key = lambda (word, showtimes, length): -showtimes))
    for idx in xrange(len(top_bigram_and_trigram_pair_list)):
        pair_tuple = top_bigram_and_trigram_pair_list[idx]
        word = pair_tuple[0]
        showtimes = pair_tuple[1]
        length = pair_tuple[2]
        logging.info("idx+1:%s, word:%s, showtimes:%s, length:%s" % (idx+1, word, showtimes, length))

    pair_num = ngram_pair_rdd.count()
    logging.info("pair_num:%s" % pair_num)
    logging.info("ngram_pair_rdd.take(1):%s" % ngram_pair_rdd.take(1))
    logging.info("preparing to insert ngram to table %s of database %s."\
                 % (ngram_table_name, word_database_name))
    ngram_pair_rdd\
        .map(lambda (word, showtimes, length):\
                         insert_ngram_2_db
                             (word = word,\
                              showtimes = showtimes,\
                              database_name = word_database_name,\
                              table_name = ngram_table_name\
                             )\
             )\
        .take(ngram_pair_rdd.count())

    computation_corpus_scale(database_name = word_database_name, table_name = ngram_table_name)

    sc.stop()
    logging.info("END at:" + time.strftime('%Y-%m-%d %X', time.localtime()))
################################ PART4 EXECUTE ########################################
if __name__ == "__main__":
    main()