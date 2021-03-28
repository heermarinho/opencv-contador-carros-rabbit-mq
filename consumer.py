import os
import pickle

from console_logging.console import Console
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

from lgetter.Cartracking.CarTracker import CarTracker

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
        self.car_tracker = CarTracker()

    def process_cartracking(self, frame, operation, camera):
        _, moving_car = self.car_tracker.track(frame,operation, camera)

        return moving_car

    def on_message(self, body, message):
        json_input = pickle.loads(body)
        print(json_input)
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
