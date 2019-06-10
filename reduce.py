import cosbackend
import pika
import json

def main(args):
    result = {}
    messages = 0
    maps = args['maps']
    def callback(ch, method, properties, body):
        nonlocal result
        nonlocal messages
        nonlocal maps
        data = json.loads(body)
        result = {x: result.get(x,0) + data.get(x, 0) for x in set(result).union(data)}
        messages += 1
        if messages == maps:
            ch.stop_consuming()

    params = pika.URLParameters(args['config']['rabbit_mq']['rabbit_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.basic_consume(callback,
                          queue='reduce',
                          no_ack=True)
    channel.start_consuming()
    cos = COSBackend(args['config']['ibm_cos'])
    cos.put_object(args['config']['ibm_cos']['bucket'], args['key'], json.dumps(result))
    connection.close()
    return ({'result':"OK"})
