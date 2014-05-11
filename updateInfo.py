# -*- coding: utf-8 -*-
import csv
import os
import sys
import pika
import time
from douban_book_api import getbyisbn_without_auth
from loggerinit import initialize_logger

log = initialize_logger(os.getcwd(),'fetchFromDouban')
reload(sys)
sys.setdefaultencoding("utf-8")
path = sys.argv[1]

#set up message queue, and made queue durable
connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
channel = connection.channel()

channel.queue_declare(queue='douban_queue', durable=True)

SEPERATOR = '\001'

douban_file = open(path, 'a')
writer = csv.writer(douban_file, quoting=csv.QUOTE_ALL)
skiplist = ['9787540449902']

def fetchFromDouban(ch, method, properties, body):
    flag = True
    info = body.split(SEPERATOR)

    while(True):
        try:
            log.info(info[1])
            data = getbyisbn_without_auth(info[1])
            break
        except Exception, e:
            log.error("Connection error happened: "+str(e))
            log.error("The isbn is {}".format(info[1]))
            if info[1] in skiplist:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            time.sleep(30)

    if 'code' in data:
        print str(data['code'])+data['msg']
        flag = False
        if data['code']==6000:
            log.warn("Cannot find the book, isbn is {}".format(info[1]))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            log.error("Error happend, the spu id is {} and isbn is".format(info[0],info[1]))
            raise Exception("unknow error")


    if flag is True:
        author = ",".join(data['author'])
        translator = ",".join(data['translator'])
        tags = []
        for tag in data['tags']:
            tagStr = ":".join([tag['name'], str(tag['count'])])
            tags.append(tagStr)
        tagString = ';'.join(tags)

        list = [info[0], data['id'], data['isbn10'], data['isbn13'], data['title'], data['origin_title'],
                data['alt_title'], data['subtitle'],
                data['image'], author, translator, data['publisher'], data['pubdate'], data['rating']['average'],
                data['rating']['numRaters'], tagString, data['binding'],
                data['price'], data['pages'],data['author_intro'].replace('\n', '\t'), data['summary'].replace('\n', '\t'),time.strftime('%Y-%m-%d %H:%M:%S')]
        writer.writerow(list)
        ch.basic_ack(delivery_tag=method.delivery_tag)


#start comsume message
channel.basic_qos(prefetch_count=1)
channel.basic_consume(fetchFromDouban,
                      queue='douban_queue')

channel.start_consuming()

