import os
import pickle

from console_logging.console import Console
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

import dataset 
print("import dataset heheheheh")
# https://dataset.readthedocs.io/en/latest/ setar para falar com banco
db = dataset.connect('mysql://root:asdqwe123@192.168.0.108:49153/COUNTER_TBL')
tabela = db['EVENTOS']

console = Console()
queue = "contador-carro-exchange"
exchange = "contador-carro-exchange"
routing_key = "contador-carro-exchange"
rabbit_url = "amqp://guest:guest@192.168.0.108:5672//"

# Rabbit config
conn = Connection(rabbit_url)
channel_ = conn.channel()
exchange_ = Exchange(exchange, type="direct", delivery_mode=1)



class Worker(ConsumerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def on_message(self, body, message):
        data = pickle.loads(body)
        print(data['id_camera'])
        tabela.insert(dict(id_camera=data['id_camera'], data_=data['data_'], dt=data['dt'],evento=data['evento'])) 
        message.ack()

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message])]


def run():
    console.info("[ CONSUMER - WORKER ]  QUEUE: %s " % queue)
    queues = [Queue(queue, exchange_, routing_key=routing_key)]
    with Connection(rabbit_url, heartbeat=80) as conn:
        worker = Worker(conn, queues)
        console.info("[ CONSUMER - WORKER ]  WORKER RUNNING ")
        worker.run()


if __name__ == "__main__":
    console.info("[ CONSUMER - WORKER ] ....STARTED.... ")
    run()
