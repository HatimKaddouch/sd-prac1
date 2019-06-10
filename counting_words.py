import cosbackend
import pika
import json

def main(args):
    c = 0
    bytes = get_object(args['bucket_name'], args['key'],
        extra_get_args={'Range':"bytes="+args['lower']+"-"+args['upper']})
    bytes = bytes.decode("utf-8")
    bytes = bytes.split("\n")
    for line in bytes:
        line = line.split(" ")
        c += len(line)
    dict = {'words':c}
    params = pika.URLParameters(args['rabbit_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.basic_publish(exchange='',
                          routing_key='reduce',
                          body=json.dumps(dict))
    return({'result':"OK"})
