from confluent_kafka import Producer
import json
import time
from kafka_server_config import *
class InvoiceProducer:
    def __init__(self):
        self.topic = 'demo_invoices'
        self.conf = {'bootstrap.servers' : bootstrap_server_url,
                     'security.protocol' : 'SASL_SSL',
                     'sasl.mechanism' : 'PLAIN',
                     'sasl.username' : username,
                     'sasl.password' : password,
                     'client.id' : 'shivam-workbook'}


    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery : {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))['InvoiceNumber']
            print(f"Produced event to : key = {key} value = {invoice_id}")

    def produce_invoices(self,producer,counts):
        counter = 0
        with open("data/invoices.json") as invoices:
            for line in invoices:
                invoice = json.loads(line)
                store_id = invoice['StoreID']
                producer.produce(self.topic, key = store_id, value = line, callback = self.delivery_callback)
                time.sleep(0.5)        # just to slow down the process
                producer.poll(1)        # to wait 1 sec for acknowledgement for each invoice sent
                counter = counter + 1
                if counter == counts:     # to limit the no of invoices sent to kafka
                    break

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer,10)
        kafka_producer.flush(10)


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()

