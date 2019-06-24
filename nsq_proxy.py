import os

from asyncnsq import create_nsq_producer, create_nsq_consumer


class NSQ:
    def __init__(self, topic='orders'):
        self.topic = topic
        self.host = os.getenv('NSQD_HOST', '127.0.0.1')
        self.port = os.getenv('NSQD_PORT', '4150')

    async def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class NSQWriter(NSQ):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writer = None

    async def open(self):
        self.writer = await create_nsq_producer(host=self.host,
                                                port=self.port)
        return self

    def close(self):
        self.writer.close()

    async def publish(self, message):
        await self.writer.pub(topic=self.topic,
                              message=message)


class NSQReader(NSQ):
    def __init__(self, *args, channel='orders', **kwargs):
        super().__init__(*args, **kwargs)
        self.channel = f'{self.topic}_{channel}'
        self.reader = None

    async def open(self):
        address = f'tcp://{self.host}:{self.port}'
        self.reader = await create_nsq_consumer(host=[address])
        return self

    async def subscribe(self):
        await self.reader.subscribe(topic=self.topic, channel=self.channel)

    def close(self):
        self.reader.close()
