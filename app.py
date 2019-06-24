import asyncio
import random
from contextlib import closing

from loggers import log
from nsq_proxy import NSQReader, NSQWriter


async def process_order(order):
    await asyncio.sleep(random.randint(3, 10))
    log.info(f'Order {order.body} has been processed')
    # writer = await NSQWriter(topic='processed_orders').open()
    # await writer.publish(order.body)
    # log.info(f'Dish passed to the waiter')


async def watch():
    log.info('Waiting for orders')
    reader = await NSQReader(topic='client_orders').open()
    await reader.subscribe()
    for waiter in reader.reader.wait_messages():
        message = await waiter
        log.info(f'Received order: {message.body}')
        await process_order(message)
        await message.fin()
    reader.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(watch())
    loop.run_forever()
