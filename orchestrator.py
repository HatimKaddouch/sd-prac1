class Orchestrator:
    def _init_(self, key, maps, config):
        self.key = key
        self.maps = maps
        self.config = config
        self.cos = COSBackend(config['ibm_cos'])
        self.cf = CloudFunctions(config['cloud_functions'])

        params = pika.URLParameters(args['config']['rabbit_mq']['rabbit_url'])
        self.connection = pika.BlockingConnection(params)
        self.channel = connection.channel() # start a channel
        channel.queue_declare(queue='reduce') # Declare a queue

    def map_reduce(function):
        key_meta = self.cos.head_object(self.config['ibm_cos']['bucket'])
        size = key_meta['content-length']
        size_part = size // maps

        for i in range(maps):
            cf.invoke(function, args)

        cf.invoke_with_result('reduce', args)

        result = cos.get_object(self.config['ibm_cos']['bucket'])
        self.channel.queue_delete(queue='reduce')
        self.connection.close()
        return(result)
