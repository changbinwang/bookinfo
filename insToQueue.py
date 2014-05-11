import csv
import os
from MySQLdb.cursors import Cursor, DictCursor
import MySQLdb as mdb
import pika
from loggerinit import initialize_logger


log = initialize_logger(os.getcwd(),'insertIntoQueue')
#set up message queue, and made queue durable
connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
channel = connection.channel()

channel.queue_declare(queue='douban_queue', durable=True)

SEPERATOR='\001'

#fetch data from db, may be changed later for data refreshment
def insertFromDB(tableName):
    con = mdb.connect('localhost', 'root', 'mdhwoaini', 'odps', charset='utf8')
    with con:
        cur = con.cursor(DictCursor)
        cur.execute(
            "select auction_id,isbn,isbn_10 from {} where isbn is not null or isbn_10 is not null order by auction_id".format(tableName))
        rows = cur.fetchall()
        count = 0
        for row in rows:
            if row['isbn'] is not None:
                message = str(row['auction_id'])+SEPERATOR+row['isbn']
            else:
                message = str(row['auction_id'])+SEPERATOR+row['isbn_10']

            channel.basic_publish(exchange='',
                                  routing_key='douban_queue',
                                  body=message,
                                  properties=pika.BasicProperties(delivery_mode=2,))
            count+=1
            if count%1000==0:
                log.info("I imported {} into queue".format(count))

        connection.close()

def insertFromFile(fileName):
    with open(fileName,'rb') as file:
        reader = csv.reader(file)
        count = 0
        for row in reader:
            values=row[0].split('\t')
            if values[1]!='NULL':
                message = values[0]+SEPERATOR+values[1]
            else:
                 message = values[0]+SEPERATOR+values[2]

            channel.basic_publish(exchange='',
                                  routing_key='douban_queue',
                                  body=message,
                                  properties=pika.BasicProperties(delivery_mode=2,))
            count+=1
            if count%1000==0:
                log.info("I imported {} into queue".format(count))
        connection.close()


insertFromFile('/Users/owen2785/spu.txt')

