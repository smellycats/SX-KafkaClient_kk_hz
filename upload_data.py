# -*- coding: utf-8 -*-
import time
import json
import socket

import arrow

import helper
#from helper_consul import ConsulAPI
from helper_kafka_consumer import KafkaConsumer
from helper_kafka_producer import KafkaProducer
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kc = None
        self.kp = KafkaProducer(**dict(self.my_ini['kafka_producer']))
        #self.con = ConsulAPI()
        #self.con.path = self.my_ini['consul']['path']

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

        self.item = None
        self.part_list = list(range(60))

    def handling_data(self):
        info = []
        offsets = {}
        for i in range(500):
            msg = self.kc.c.poll(0.002)
        
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                try:
                    i = json.loads(msg.value().decode('utf-8'))
                    info.append(i)
                except Exception as e:
                    logger.error(e)
                    logger.error(msg.value())
                    time.sleep(15)
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        if offsets == {}:
            return 0
        else:
            lost_msg = []             # 未上传数据列表
            def acked(err, msg):
                if err is not None:
                    lost_msg.append(msg.value().decode('utf-8'))
                    logger.error(msg.value())
                    logger.error(err)
            t = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
            for i in info:
                self.kp.produce_info(key=None, value=json.dumps(i), cb=acked)
            self.kp.flush()
            if len(lost_msg) > 0:
                return len(lost_msg)
            self.kc.c.commit(async=False)
            info_msg = 'info={0}, lost_msg={1}, offset={2}'.format(len(info), len(lost_msg), offsets)
            print(info_msg)
            logger.info(info_msg)
            return 0

    def main_loop(self):
        while 1:
            try:
                #if not self.get_lock():
                #    if self.kc is not None:
                #        del self.kc
                #        self.kc = None
                #    self.item = None
                #    self.part_list = []
                #    time.sleep(2)
                #    continue
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka_consumer']))
                    self.kc.assign(self.part_list)
                n = self.handling_data()
                if n > 0:
                    time.sleep(15)
            except Exception as e:
                logger.exception(e)
                time.sleep(15)

        
