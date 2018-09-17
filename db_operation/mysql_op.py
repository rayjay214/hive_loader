#!/usr/bin/env python
# coding=utf-8

import configparser
import dataset
import json
from collections import OrderedDict
import logging

logger = logging.getLogger('consumer')

class MySqlOperation:
    def __init__(self):
        self._conf = self.setup_conf()
        self._conn = dataset.Database(self._conf.get('mysql', 'connect_str'))
        self._table_meta_dict = {}

    # to do extract a common module for config
    def setup_conf(self):
        conf = configparser.ConfigParser()
        conf.read('db_operation/db.conf')
        #conf.read('db.conf')
        return conf
   
    # query meta table in mysql and save it locally
    def load_table_meta(self):
        meta_table_name = self._conf.get('mysql', 'meta_table_name')
        sql = '''select * from {}'''.format(meta_table_name)
        result = self._conn.query(sql)
        for row in result:
            key = row['tablename']
            self._table_meta_dict[key] = row
            #transform json str into data structure
            try:
                self._table_meta_dict[key]['coldescription'] = json.loads(self._table_meta_dict[key]['coldetail'], object_pairs_hook=OrderedDict)
            except Exception as e:
                logger.error('json loads failed, data is {}'.format(self._table_meta_dict[key]['coldetail']))
                logger.error('error {}'.format(e))

    def get_table_meta_dict(self):
        return self._table_meta_dict

    # fetch the specified table meta dict
    def get_table_meta(self, table):
        dict = {}
        meta_table_name = self._conf.get('mysql', 'meta_table_name')
        sql = '''select * from {} where tablename='{}' '''.format(meta_table_name, table)
        result = self._conn.query(sql)
        #only one row
        for r in result:
            dict = r
        if dict is None:
            logger.error('{} table meta is none'.format(table))
            return None
        try:
            dict['coldescription'] = json.loads(dict['coldetail'], object_pairs_hook=OrderedDict)
        except:
            logger.error('json load coldetail failed, table name is {} coldetail is {}'.format(table, dict['coldetail']))
            return None
        return dict

    # Test
    
    def get_seq_dict(self, coldescription):
        dict = {}
        for idx, col in enumerate(coldescription):
            dict[idx] = coldescription[col]['seq']
        return dict
   
    def get_seq(self, dict, idx):
        return dict.get(idx, -1)

'''
#test
op = MySqlOperation()
op.load_table_meta()

meta = op.get_table_meta_dict()
meta = op.get_table_meta('t_user_opt')
coldescription = meta['t_user_opt']['coldescription']
dict = op.get_seq_dict(coldescription)
print(dict)
for idx, item in enumerate(dict):
    seq = op.get_seq(dict, idx)
    print(seq)

meta = op.get_table_meta('t_user_opt')
coldescription = meta['coldescription']
for col in coldescription:
    col_value = coldescription[col]
    print('col:{}, type:{}'.format(col, col_value['type']))
'''



