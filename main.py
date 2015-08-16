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
    essay_table_name = "securities_newspaper_shzqb_table"

    # table1: securities_newspaper_shzqb_table
    # table2: securities_newspaper_zgzqb_table
    # table3: securities_newspaper_zqrb_table
    # table4: securities_newspaper_zqsb_table

    GetCorpus = class_get_corpus_from_db(database_name = essay_database_name)
    GetCorpus.get_essay_list_from_db(database_name = essay_database_name, table_name = essay_table_name)











    logging.info("END at:" + time.strftime('%Y-%m-%d %X', time.localtime()))
################################ PART4 EXECUTE ########################################
if __name__ == "__main__":
    main()