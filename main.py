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

################################### PART2 MAIN && FUNCTION ############################
def main():
    # Log part
    logging.basicConfig(level = logging.DEBUG,
              format = '%(asctime)s %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
              datefmt = '%y-%m-%d %H:%M:%S:%SS',
              filename = './main.log',
              filemode = 'a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logging.info("START at " + time.strftime('%Y-%m-%d %X', time.localtime()))

    # Initialize parameters
    database_name = "wordsDB"
    table_name = "ngram_word_table"

    CreateDBandTable = class_create_database_table()
    CreateDBandTable.create_database(database_name = database_name)
    CreateDBandTable.create_table(database_name = database_name, table_name = table_name)



    logging.info("END at:" + time.strftime('%Y-%m-%d %X', time.localtime()))
################################ PART4 EXECUTE ########################################
if __name__ == "__main__":
    main()