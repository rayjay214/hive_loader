#!/usr/bin/env python
# coding=utf-8

from pyhive import hive
import re
import configparser
import mysql_op
from collections import OrderedDict
import logging

logger = logging.getLogger('consumer')

eliminate_str = ('ROW', 'STORED', 'LOCATION', 'TBLPROPERTIES', 'OUTPUTFORMAT')

class HiveOperation:
    def __init__(self):
        self._conf = self.setup_conf()
        self._conn = hive.connect(self._conf.get('hive', 'host'))
        self._cursor = self._conn.cursor()

    def setup_conf(self):
        conf = configparser.ConfigParser()
        conf.read('db_operation/db.conf')
        return conf

    def get_create_tbl_sql(self, table):
        db_name = self._conf.get('hive', 'db_name')
        sql = '''CREATE TABLE {}.{} ( '''.format(db_name, table)
        mysqlOperation = mysql_op.MySqlOperation()
        table_meta = mysqlOperation.get_table_meta(table)
        if table_meta is None:
            logger.error('{} table meta is None'.format(table))
            return None

        coldescription = table_meta['coldescription']
        for col in coldescription:
            sql += '''`{}` '''.format(col['name'])
            type = col['type']
            #if 'char' in type.lower() or 'text' in type.lower() or 'timestamp' in type.lower():
            if 'char' in type.lower() or 'text' in type.lower():
                type = 'STRING'
            sql += type+', '
        #eliminate the last ', '
        sql = sql[:-2]
        sql += ' ) '
        sql += ''' PARTITIONED BY (year INT, month INT, day INT) '''
        return sql

    def create_table(self, table):
        sql = self.get_create_tbl_sql(table)
        if sql is None:
            logger.error('create table {} failed'.format(table))
            return -1

        format = self._conf.get('hive', 'table_fmt')        
        sql += 'STORED AS ' + format
        logger.info('create table sql is {}'.format(sql))
        try:
            self._cursor.execute(sql)
        except Exception as e:
            logger.error("create table failed, error is {}".format(e))
            return -1
        return 0
    
    def alter_table(self, table, delta_dict):
        sql = self.get_alter_table_sql(table, delta_dict)
        try:
            self._cursor.execute(sql)
        except Exception as e:
            logger.error('alter table failed, error is {}'.format(e))
            return -1
        return 0

    def get_alter_table_sql(self, table, delta_dict):
        sql = '''ALTER TABLE {}.{} ADD COLUMNS ('''.format(self._conf.get('hive', 'db_name'), table)
        for key in delta_dict:
            sql += key + ' ' + delta_dict[key] + ','
        #去掉最后一个','
        sql = sql[:-1]
        sql += ')'
        logger.info("alter table sql is {}".format(sql))
        return sql


    def is_table_changed(self, table):
        hive_dict = OrderedDict()
        meta_dict = OrderedDict()

        sql_from_hive = self.get_create_tbl_from_hive(table)
        sql_from_meta = self.get_create_tbl_sql(table)

        if  sql_from_hive is None:
            logger.error('sql from hive is none, table is {}'.format(table))
            return -1, None

        if sql_from_meta is None:
            logger.error('sql from meta is none, table is {}'.foramt(table))
            return -1, None

        hive_col_str = self.get_col_str(sql_from_hive)
        meta_col_str = self.get_col_str(sql_from_meta)

        logger.info('hive_col_str is {}, table is {}'.format(hive_col_str, table))
        logger.info('meta_col_str is {}, table is {}'.format(meta_col_str, table))

        hive_col_str = hive_col_str.replace('`', '')
        meta_col_str = meta_col_str.replace('`', '')
        
        hive_col_str = hive_col_str.lower()
        meta_col_str = meta_col_str.lower()

        hive_cols = hive_col_str.split(',')
        meta_cols = meta_col_str.split(',')

        logger.debug('hive_cols is {}'.format(hive_cols))
        for col in hive_cols:
            col_type = col.strip().split(' ')
            hive_dict[col_type[0]] = col_type[1]

        logger.debug('meta_cols is {}'.format(meta_cols))
        for col in meta_cols:
            col_type = col.strip().split(' ')
            meta_dict[col_type[0]] = col_type[1]

        return self.cmp_col_dict(hive_dict, meta_dict, table)
    
    def cmp_col_dict(self, hive_dict, meta_dict, table):
        '''
            比较两个字典中包含的列是否一致，不一致则返回多出的列
            简单处理，只考虑增加列的情况，即：
            两个字典不同只存在以下情况，字典2比字典1多出一列或几列，但这之前的列都是相同的
            只存在t_metadata比hive新的情况
        '''
        changed = False
        delta_dict = {}

        hive_len = len(hive_dict)
        meta_len = len(meta_dict)
        
        if hive_len == meta_len:
            changed = False
            logger.info('{} in hive and t_metadata are same'.format(table))
            return changed, delta_dict
        
        #只存在meta_len > hive_len的情况
        if hive_len > meta_len:
            logger.error(''' {} columns in hive {} are larger than 
                         that {} in t_metadata'''.format(table, hive_len, meta_len))
            return -1, None
        
        changed = True
        for key in meta_dict:
            if key in hive_dict:
                continue
            delta_dict[key] = meta_dict[key]

        logger.warn('{} has changed, changed columns are {}'.format(table, delta_dict))
        return changed, delta_dict


    def get_col_str(self, str):
        '''
            create table ( xxxxx ) xxxxx
            返回括号中间的字符串
        '''
        begin = str.find('(')
        end = str.find(')')
        return str[begin+1:end].strip()



    def is_table_exist(self, table):
        '''
            check if the table exist in specified database
        '''
        db_name = self._conf.get('hive', 'db_name')
        sql = '''SHOW TABLES LIKE '{}' '''.format(table)

        try:
            rst_list = None
            logger.info('check table exsit sql is {}'.format(sql))
            self._cursor.execute('use ' + db_name)
            self._cursor.execute(sql)
            rst_list = self._cursor.fetchall()
            #not empty, found table
            if rst_list:
                return True
            #list is empty
            else:
                return False
        except Exception as e:
            logger.error('''check table exist sql execute failed, 
                sql is {} rst_list is {} error is {}'''.format(sql, rst_list, e))
            return -1

    def load_hdfs_file_into_tmp_table(self, file_path, table, index):
        tmp_table_sql = self.get_create_temp_tbl_sql(table, index)
        if tmp_table_sql is None:
            logger.error('get create temp table sql failed')
            return -1

        load_to_temp_sql = '''LOAD DATA INPATH '{}' INTO TABLE {}_temp{}'''.format(file_path, table, index)
        logger.info('sql1 is {}'.format(tmp_table_sql))
        logger.info('sql2 is {}'.format(load_to_temp_sql))
        try:
            self._cursor.execute(tmp_table_sql)
            self._cursor.execute(load_to_temp_sql)
        except Exception as e:
            logger.error('load data execute failed')
            logger.error('error is {}'.format(e))

    def load_tmp_table_to_main(self, table, index):
        #mysqlOperation = mysql_op.MySqlOperation()
        #table_meta = mysqlOperation.get_table_meta(table)
        
        sql = '''FROM {}_temp{} t 
            INSERT INTO TABLE {} PARTITION(year, month, day) 
            select t.*,
            year(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')),
            month(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')), 
            day(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'))
            '''.format(table, index, table)
        
        #sql = ''' FROM {}_temp{} t 
        #     INSERT INTO TABLE {} PARTITION(year, month, day) 
        #     select '''.format(table, index, table)
        
        #coldescription = table_meta['coldescription']
        #for col in coldescription:
        #    sql += '''t.`{}`, '''.format(col['name'])
        
        #sql += '''
        #    year(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')),
        #    month(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')), 
        #    day(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'))
        #'''

        logger.info('tmp to main:sql is {}'.format(sql))
        try:
            #self._cursor.execute('use test')
            db = self._conf.get('hive', 'db_name') 
            self._cursor.execute('use {}'.format(db))
            self._cursor.execute('set hive.exec.dynamic.partition.mode=nonstrict')
            self._cursor.execute('add jar /opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/hive/lib/hive-contrib.jar')
            self._cursor.execute(sql)
        except Exception as e:
            logger.error('to main:execute failed, table is {}, error is {}'.format(table, e))
            return -1

        return 0


    def get_create_temp_tbl_sql(self, table, index):
        '''
            generate sql for creating temporary table
        '''
        db_name = self._conf.get('hive', 'db_name')
        schema_sql = 'show create table {}'.format(table)

        cursor = self._conn.cursor()
        try:
            cursor.execute('use {}'.format(db_name))
            cursor.execute(schema_sql)
            rst = cursor.fetchall()
            logger.debug(rst)
        except Exception as e:
            logger.error('''show create table execute failed, 
                error is {}, sql is {}'''.format(e, schema_sql))
            return None

        create_sql_ori = ''
        for i in rst:
            for j in i:
                if not j.startswith(eliminate_str):
                    create_sql_ori += j
        match = re.match(r'CREATE TABLE(.*?)\((.*?)\)', create_sql_ori, re.M)
        if match is None:
            return None
        create_sql = match.group()
        create_sql = create_sql.replace('TABLE', 'TEMPORARY EXTERNAL TABLE', 1)
        create_sql = create_sql.replace(table, table+'_temp{}'.format(index), 1)
        #create_sql = create_sql[:-1] + ''', recv_time STRING )'''
        #create_sql = create_sql + ''' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE'''
        create_sql = create_sql + '''
            ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
            WITH SERDEPROPERTIES ("field.delim"="{}") 
            STORED AS TEXTFILE
        '''.format('(|)')
        return create_sql
   
    def get_create_tbl_from_hive(self, table):
        schema_sql = 'SHOW CREATE TABLE {}'.format(table)
        cursor = self._conn.cursor()
        try:
            cursor.execute(schema_sql)
            rst = cursor.fetchall()
            logger.debug(rst)
        except:
            logger.error("show create table execute failed")
            return None

        create_sql_ori = ''
        for i in rst:
            for j in i:
                if not j.startswith(eliminate_str):
                    create_sql_ori += j
        match = re.match(r'CREATE TABLE(.*?)\((.*?)\)', create_sql_ori, re.M)
        if match is None:
            return None
        create_sql = match.group()
        return create_sql
   
