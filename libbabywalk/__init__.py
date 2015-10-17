import pika
import json
import logging

SEED_CRAWLER_QUEUE = 'seed-crawler'
SEED_REPORT_QUEUE = 'seed-report'
CRAWLER_REPORT_QUEUE = 'crawler-report'


def create_queue(channel, name):
    logging.info('creating (queue %s)', name)
    channel.queue_declare(queue=name,
                          durable=True,
                          exclusive=False,
                          auto_delete=False)

def send_message(channel, name, message):
    logging.info('sending (queue %s) message %s', name, message)
    channel.basic_publish(exchange='',
                          routing_key=name,
                          body=json.dumps(message,
                                          ensure_ascii=False),
                          properties=pika.BasicProperties(
                              content_type='application/json'))
