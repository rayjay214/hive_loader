#!/usr/bin/env python
# coding=utf-8

import sys
import threading
import multiprocessing
from confluent_kafka import Consumer, KafkaError
import json
from collections import OrderedDict
import os
from db_operation import hive_op
from db_operation import mysql_op
from pywebhdfs.webhdfs import PyWebHdfsClient
import shutil
import configparser
import logging
import time
import traceback
import urllib

#######INIT LOG################
logging.basicConfig(format='%(process)d %(name)s %(asctime)s [%(levelname)s] %(filename)s:%(lineno)s %(funcName)s %(message)s',
    level=logging.INFO)
logger = logging.getLogger('consumer')
###############################


class Master(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._conf = self.setup_conf()
        self._hiveOp = hive_op.HiveOperation()

    def setup_conf(self):
        conf = configparser.ConfigParser()
        conf.read('etc/master.conf')
        return conf

    def load_data_into_hive(self, index):
        '''
            将data/*.tmp 数据加载到hive中去
        '''
        hiveOper = hive_op.HiveOperation()
        files = self.get_files(index, '.tmp')

        if files is None:
            logger.info('no files need to load')
            return
        
        logger.info('event set flag is {}'.format(self._consumer_jobs[index].event.is_set()))

        for file in files:
            # 文件名即表名
            table = os.path.splitext(file)[0]

            # 判断表在hive中是否存在
            ret = hiveOper.is_table_exist(table)
            logger.debug("check {} exist result is {}".format(table, ret))
            # 表已经存在，直接加载数据
            if ret:

                # 如果存在，判断表结构是否有更新
                delta_dict = {}
                ret, delta_dict = hiveOper.is_table_changed(table)

                #异常情况, 应该为t_meta表的问题
                if ret == -1:
                    logger.error('{} has some error, pls check'.format(table))
                    continue

                #表结构改变
                if ret: 
                    ret = hiveOper.alter_table(table, delta_dict)
                    #修改表失败
                    if ret == -1:
                        logger.error('alter table failed')
                        continue

                #表结构未改变
                else:
                    ret = self.files_to_hive(file, table, index)

            # 表不存在，先创建表，再加载数据
            else:
                ret = hiveOper.create_table(table)
                if ret == -1:
                    logger.error('create table {} failed, will not load files into hive'.format(table))
                    continue

                ret = self.files_to_hive(file, table, index)

            # 成功导入到hive表后，删除临时文件
            # 未成功导入，下次再继续处理
            if ret == 0:
                self.mv_temp_file(file, index)

    def files_to_hive(self, file, table, index):
        hiveOper = hive_op.HiveOperation()
        hdfs_path = self.upload_to_hdfs(file, table, index)
        if hdfs_path is None:
            logger.error('{} upload tmp files to hdfs failed, try next time'.format(table))
            return -1
        logger.info('upload finished, hdfs path is {}'.format(hdfs_path))

        ret = hiveOper.load_hdfs_file_into_tmp_table(hdfs_path, table, index)
        if ret == -1:
            logger.error('{} load from hdfs to tmp table failed'.format(table))
            return -1
        logger.info('{} load into tmp finished'.format(table))

        ret = hiveOper.load_tmp_table_to_main(table, index)
        if ret == -1:
            logger.error('{} from tmp to main failed'.format(table))
            return -1

        logger.info('{} load tmp table to main finished'.format(table))

        return 0  

    #alternatives for uploading file from local to hdfs
    #not used for now, but easy for test
    def upload_to_hdfs_bak(self, local_file, table, index):
        local_dir = self._conf.get('local', 'data_dir')
        local_path = '{}{}/{}'.format(local_dir, index, local_file)
        
        hdfs_base_path = self._conf.get('hdfs', 'upload_path')
        #hdfs_dir_path = '{}{}'.format(hdfs_base_path, index)
        hdfs_path = '{}{}/{}'.format(hdfs_base_path, index, local_file)

        logger.info('begin to upload from {} to {}'.format(local_path, hdfs_path))

        os.popen('''hdfs dfs -copyFromLocal '{}' '{}' '''.format(local_path, hdfs_path)) 
        
        t = time.time()
        hdfs_bak_path = '{}.{}'.format(hdfs_path, t)
        
        logger.info('back path is {}'.format(hdfs_bak_path))

        os.popen('''hdfs dfs -cp '{}' '{}' '''.format(hdfs_path, hdfs_bak_path))

        return hdfs_path

    def upload_to_hdfs(self, local_file, table, index):
        '''
            upload file from local filesystem to hdfs
        '''
        hiveOper = hive_op.HiveOperation()
        local_dir = self._conf.get('local', 'data_dir')
        local_path = '{}{}/{}'.format(local_dir, index, local_file)
        host1 = self._conf.get('hdfs', 'name_node1')
        host2 = self._conf.get('hdfs', 'name_node2')
        user = self._conf.get('hdfs', 'user')
        port = self._conf.getint('hdfs', 'port')
        hdfs_base_path = self._conf.get('hdfs', 'upload_path')
        hdfs_dir_path = '{}{}'.format(hdfs_base_path, index)
        hdfs_path = '{}{}/{}'.format(hdfs_base_path, index, local_file)
    
        #implement HA manually
        try:
            hdfs_cli = PyWebHdfsClient(host=host1, port=port, user_name=user)
            hdfs_cli.list_dir('/')
        except Exception as e:
            logger.warn('open hdfs client failed error {}'.format(e))
            hdfs_cli = PyWebHdfsClient(host=host2, port=port, user_name=user)
            hdfs_cli.list_dir('/')

        if hdfs_cli is None:
            logger.error('no active host')
            return None

        try:
            hdfs_cli.get_file_dir_status(hdfs_path)

            # 若hdfs中临时文件存在，表示可能是上次上传hive失败，或者进程中途被杀导致
            # 先将临时文件中的数据导入hive，再进行下一步操作
            ret = hiveOper.load_hdfs_file_into_tmp_table(hdfs_path, table)
            if ret == -1:
                logger.error('load from hdfs to tmp table failed')

            logger.info('last time! {} load into tmp finished'.format(table))
            hiveOper.load_tmp_table_to_main(table)
            logger.info('last time! {} load tmp table to main finished'.format(table)) 

        #FileNotFountException
        except Exception as e:
            #文件不存在是正常情况
            logger.debug('no such file {}'.format(hdfs_path))
            
        retry_count = 0
        upload_finished = False
        while retry_count <= 10 and not upload_finished: 
            with open(local_path) as f:
                logger.debug('''local path is {}, hdfs_cli is {}, 
                    file is {}, hdfs_path is {}'''.format(local_path, hdfs_cli, f, hdfs_path))
                #hdfs_cli.delete_file_dir(hdfs_path)

                #若目录不存在，先创建目录
                try:
                    hdfs_cli.get_file_dir_status(hdfs_dir_path)    
                except Exception as e:
                    hdfs_cli.make_dir(hdfs_dir_path)

                try:
                    hdfs_cli.create_file(hdfs_path, f)
                    upload_finished = True
                except Exception as e:
                    logger.warn('''create file on hdfs failed, 
                        local path is {}, hdfs path is {}, 
                        retry count {}, upload flag {}'''.format(local_path, hdfs_path, retry_count, upload_finished))
                    logger.warn('error is {}'.format(e))
                    retry_count +=1
        
        if retry_count <= 10:
            return hdfs_path
        else:
            logger.error('''{} upload 10 times, still failed, 
                retry count {}, upload_flag is {}'''.format(local_path, retry_count, upload_finished))
            return None


    def get_files(self, index, suffix):
        '''
            return *.xxx file list
        '''
        dir = 'data/{}/'.format(index)
        files=[]
        try:
            f_list = os.listdir(dir)
        except Exception as e:
            logger.error('no files error {}'.format(e))
            return None
        
        execlude_files = []

        #先遍历一遍看有没有上次上传失败的
        for file in f_list:
            #logger.info('tttt file is {} {}'.format(os.path.splitext(file)[0], os.path.splitext(file)[1]))
            if os.path.splitext(file)[1] == '.tmpupload':
                logger.warn('found last time upload failed file {}'.format(file))
                files.append(file)
                execlude_files.append(os.path.splitext(file)[0])

        for file in f_list:
            #遇到上次上传失败的表，先上传上次的，跳过本次的
            filename = os.path.splitext(file)[0]
            if filename in execlude_files:
                logger.warn('{} last time upload failed'.format(filename))
                continue

            if os.path.splitext(file)[1] == suffix:
                file_path = '{}{}'.format(dir, file)
                new_file = '{}upload'.format(file)
                new_file_path = '{}upload'.format(file_path)
                logger.debug('new file path is {}'.format(new_file_path))
                os.rename(file_path, new_file_path)
                logger.info('rename from {} to {}'.format(file_path, new_file_path))		
                files.append(new_file)
        return files
    
    
    def mv_temp_file(self, file, index):
        '''
            mv files to /tmp/record_log/index/
        '''
        tmp_dir = self._conf.get('local', 'tmp_dir')
        tmp_dir_path = '{}{}'.format(tmp_dir, index)
        path = '{}{}/{}'.format(tmp_dir, index, file)
        try:
            logger.warn('about to remove file, path is {}'.format(path))
            os.remove(path)
        except Exception as e:
            logger.warn('remove {} failed, error is {}'.format(path, e))
        
        shutil.move('data/{}/{}'.format(index, file), tmp_dir_path)

    def timer_task(self, index=None):
        #self._consumer_jobs[index].cancel()
        logger.info('timer start')
        self.load_data_into_hive(index)
        #self._consumer_jobs[index].restart()

    def run(self):
        self._timer=RepeatTimer(self._conf.getint('local', 'interval'), self.timer_task)
        self._timer.start()
        
        count = self._conf.getint('local', 'process_count')
        self._consumer_jobs = []
        
        for i in range(count):
            consumer = RecordConsumer(i)
            self._consumer_jobs.append(consumer)

        for job in self._consumer_jobs:
            job.start()

        for job in self._consumer_jobs:
            job.join()

        self._timer.join()

        
class RepeatTimer(threading.Thread):
    def __init__(self, interval, callable, *args, **kwargs):
        threading.Thread.__init__(self)
        self.interval = interval
        self.callable = callable
        self.args = args
        self.kwargs = kwargs
        self.event = threading.Event()
        self.event.set()

    def run(self):
        #to do, change later
        conf = configparser.ConfigParser()
        conf.read('etc/master.conf')
        count = conf.getint('local', 'process_count')      

        while self.event.is_set():
            timers = []
            for i in range(count):
                timer = threading.Timer(self.interval, self.callable,
                    self.args, {'index':i})
                timers.append(timer)

            #每隔一分钟启动一个定时器
            for job in timers:
                job.start()
                time.sleep(10)

            for job in timers:
                job.join()


    def cancel(self):
        self.event.clear()

class RecordConsumer(multiprocessing.Process):
    '''
        _index表示consumer在master中的索引
        用于在data目录中创建不同的目录
    '''
    def __init__(self, index):
        multiprocessing.Process.__init__(self)
        self._index = index
        self.event = threading.Event()
        self.event.set()
        self._conf = self.setup_conf()
        self.setup_dir()
        self._mysql_op = mysql_op.MySqlOperation()
        self._mysql_op.load_table_meta()

    def setup_conf(self):
        conf = configparser.ConfigParser()
        conf.read('etc/consumer.conf')
        return conf

    '''
        创建存放临时文件的目录
    '''
    def setup_dir(self):
        dir_path = 'data/{}/'.format(self._index)
        if os.path.isdir(dir_path):
            logger.info('dir {} exist'.format(dir_path))
        else:
            os.mkdir(dir_path)
        
        tmp_dir_path = '/tmp/record_log/{}'.format(self._index)
        if os.path.isdir(tmp_dir_path):
            logger.info('dir {} exist'.format(tmp_dir_path))
        else:
            os.mkdir(tmp_dir_path)
            logger.info('dir {} create success'.format(tmp_dir_path))
    
    #useless for now
    def cancel(self):
        logger.critical('consumer {} stoped'.format(self._index))
        self.event.clear()

    #useless for now
    def restart(self):
        logger.critical('consumer {} restarted'.format(self._index))
        self.event.set()

    def run(self):
        logger.info('consumer {} run'.format(self._index))
        conf = {}
        conf['bootstrap.servers'] = self._conf.get('kafka', 'bootstrap.servers')
        conf['group.id'] = self._conf.get('kafka', 'group.id')
        conf['default.topic.config'] = json.loads(self._conf.get('kafka', 'default.topic.config'))
        conf['log.connection.close'] = self._conf.get('kafka', 'log.connection.close')
        kafkaConsumer = Consumer(conf)
        topics = []
        topics.append(self._conf.get('kafka', 'topic'))
        kafkaConsumer.subscribe(topics)
        while True:
            msg_count = 0
            #useless event is always set
            while self.event.is_set(): 
                msg = kafkaConsumer.poll(1)
                
                msg_count += 1
                if (msg_count % 5000) == 0:
                    time.sleep(0.1)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.warn('{} [{}] reached end at offset {}'.
                            format(msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        # Error
                        logger.error('kafka receive msg error, error is {}, msg is {}'.
                            format(msg.error(), msg.value()))
                        #raise KafkaException(msg.error())
                        continue
                else:
                    #normal condition
                    #timestamp_tuple = msg.timestamp()
                    #timestamp_tuple[1] / 1000
                    try:
                        msg_value = msg.value().decode('utf-8', 'replace')
                    except Exception as e:
                        logger.error('msg decode error, error is {} msg is {}'.format(e, msg))
                        continue

                    self.msg_process(msg_value)

    def msg_process(self, msg):
        try:
            logger.debug('rawmsg is {}'.format(msg))
            records = json.loads(msg, object_pairs_hook=OrderedDict)
        except Exception as e:
            logger.error("json parse error")
            logger.error("error is {}".format(e))
            return

        try:
            #time_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            table = records['table']
            colcount = records['colcount']
            key = records['key']
            tables_meta = self._mysql_op.get_table_meta_dict()
            logger.debug('tables meta is {}'.format(tables_meta))

            if tables_meta.has_key(table):
                table_meta = tables_meta[table]
            #找不到table_meta, 表明table为新添加的，需要更新缓存
            else:
                logger.warning('msg from new table {}'.format(table))
                self._mysql_op.load_table_meta()
                tables_meta = self._mysql_op.get_table_meta_dict()
                if not tables_meta.has_key(table):
                    logger.error('''can't find table {} in t_metadata'''.format(table))
                    return
                table_meta = tables_meta[table]
            
            #msg中的列数大于缓存中的列数，表示t_metadata有更新，有增加列情况
            meta_colcount = table_meta['colcount'] 
            if colcount > meta_colcount:
                logger.warning('''msg shows that table {} column has changed, 
                    msg count is {}, cache count is {}'''.format(table, colcount, meta_colcount))
                self._mysql_op.load_table_meta()
                tables_meta = self._mysql_op.get_table_meta_dict()
                table_meta = tables_meta[table]

            logger.debug('table mata is {}'.format(table_meta))

            coldescription = table_meta['coldescription']
            seq_dict = self.get_seq_dict(coldescription, key)
            logger.debug('col description is {}'.format(coldescription))
            logger.debug('seq dict is {}'.format(seq_dict))
            value = records['record']
        except Exception as e:
            logger.error('msg parse error, msg is {}, error is {}'.format(msg, e))
            traceback.print_exc(e)
            return

        if value is None:
            logger.error('value is none');
            return

        for list in value:
            #values 会根据 'seq_dict' 中的描述重新排序
            try:
                if len(list) != colcount:
                    logger.error('''colcount in keys and values are not same, 
                        skip it, list len is {}, key col count is {}
                        list is {}'''.format(len(list), colcount, list))
                    continue

                #new_value = [0 for i in range(colcount)]
                new_value = ['']*colcount
                logger.debug('list is {}'.format(list))
                for idx, item in enumerate(list):
                    seq = self.get_seq(seq_dict, idx)
                    logger.debug('seq is {}, idx is {}, item is {}'.format(seq, idx, item))
                    #to do
                    if seq == -1:
                        logger.error('seq error, list is {}, idx is {}'.format(list, idx))
                        pass
                    #new_value[seq] = str(item).encode('string_escape').replace('\\x', '').decode('hex')
                    #"|"是column的分隔符，要替换字符串中的“|”，以防影响load结果
                    #new_value[seq] = str(item).replace('|', '***')
                    #new_value[seq] += '|'
                    new_value[seq] = urllib.unquote(str(item))
                    new_value[seq] += '(|)'
            #如果coldetail增加列，但是colcount(msg的json中带的)没有更新，可能导致数组越界
            except Exception as e:
                logger.error('msg process error {}'.format(e))
                traceback.print_exc(e)
                return    
            try:
                str_value = ''.join(new_value)
                if str_value.endswith('(|)'):
                    str_value = str_value[:-3]
            except Exception as e:
                logger.error('error is {}, new_value is {}'.format(e, new_value))
                return

            #必须保证写入到临时文件中的是一行，所以要替换各种换行符
            str_value = urllib.unquote(str_value)
            str_value = str_value.replace("\r\n", "###$$$").replace("\r", "###").replace("\n", "$$$").replace("^", "\^")
            self.write_to_file(table, str_value)
            logger.debug('whole repr str is {}'.format(repr(str_value)))
            logger.debug('whole str is {}'.format(str_value))

    def get_seq(self, dict, idx):
        return dict.get(idx, -1)

    def get_seq_dict(self, coldescription, key):
        '''
            "key":["col1","col2","col3"]
            key表示插入的顺序，可能为乱序col2，col1，col3
            需要得到col在表中的真正的顺序
        '''
        dict = {}
        temp_dict = {}
        logger.debug('coldescription is {}, key is {}'.format(coldescription, key))
        for col in coldescription:
            logger.debug('col is {}'.format(col))
            logger.debug('name:{} seq{}'.format(col['name'], col['seq']))
            temp_dict[col['name']] = col['seq']
        logger.debug('temp_dict is {}'.format(temp_dict))
        for idx, col in enumerate(key):
            dict[idx] = temp_dict[col]
        logger.debug('dict is {}'.format(dict))
        return dict

    #在不考虑列乱序的情况下，用以下两个函数
    '''
    def msg_process(self, msg):
        try:
            record = json.loads(msg, object_pairs_hook=OrderedDict)
            table = record['table']
            #colnum = record['colnum']
            coldetail = record['coldetail']
            line = self.get_line_str(coldetail)
            print(line)
            self.write_to_file(table, line)
        except json.JSONDecodeError:
            print("parse error")
    '''

    '''
    def get_line_str(self, coldetail):
        line = ''
        for item in coldetail:
            line += str(coldetail[item])
            line += '|'
        #eliminate the last '|'
        return line[:-1];
    '''

    def write_to_file(self, table, line):
        file_name = 'data/{}/{}.tmp'.format(self._index, table)
        logger.debug('filename is {}, line is {}'.format(file_name, line))
        with open(file_name, 'a') as f:
            #need to test if the output will be cached
            f.write(line + '\n')
            #f.flush()
            #os.fsync(f.fileno())
        
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    master = Master()
    master.start()
    master.join()

