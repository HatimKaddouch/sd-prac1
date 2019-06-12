from cosbackend import *
import pika
import json

def main(args):
    c = 0
    cos = COSBackend(args['config']['ibm_cos'])
    bytes = cos.get_object(args['config']['ibm_cos']['bucket'], args['key'],
        extra_get_args={'Range':"bytes="+args['lower']+"-"+args['upper']})
    bytes = bytes.decode("utf-8")
    bytes = bytes.split("\n")
    for line in bytes:
        line = line.split(" ")
        c += len(line)
    dict = {'words':c}
    params = pika.URLParameters(args['config']['rabbit_mq']['rabbit_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.basic_publish(exchange='',
                          routing_key='reduce',
                          body=json.dumps(dict))
    connection.close()
    return({'result':"OK"})
