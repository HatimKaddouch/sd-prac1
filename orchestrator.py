from cosbackend import *
from ibm_cf_connector import *
import pika

class Orchestrator:
    def __init__(self, key, maps, config):
        self.key = key
        self.maps = maps
        self.config = config
        self.cos = COSBackend(config['ibm_cos'])
        self.cf = CloudFunctions(config['cloud_functions'])


    def map_reduce(self, function):
        print("Lanzando función "+function)
        key_meta = self.cos.head_object(self.config['ibm_cos']['bucket'], self.key)
        size = key_meta['content-length']
        size_part = int(size) // self.maps
        args = {'config':self.config,'maps':self.maps,'key':self.key}
        print(self.config['rabbit_mq']['rabbit_url'])
        params = pika.URLParameters(self.config['rabbit_mq']['rabbit_url'])
        connection = pika.BlockingConnection(params)
        channel = connection.channel() # start a channel
        channel.queue_declare(queue='reduce') # Declare a queue

        for i in range(maps):
            args['lower'] = i * size_part
            if i == self.maps - 1:
                args['upper'] = size - 1
            else:
                args['upper'] = (i + 1) * size_part - 1
            self.cf.invoke(function, args)

        self.cf.invoke_with_result('reduce', args)
        print("Recogiendo resultados de "+function)
        result = self.cos.get_object(self.config['ibm_cos']['bucket'], 'result')
        self.cos.delete_object(self.config['ibm_cos']['bucket'], 'result')
        channel.queue_delete(queue='reduce')
        connection.close()
        return(result)
