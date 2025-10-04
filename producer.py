import json
import uuid

from confluent_kafka import Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f'Message delivery failed: {err   }')
    else:
        print(f'Delivered : {msg.value().decode("utf-8")}')
        print(dir(msg))
order ={
    "order_id": str(uuid.uuid4()),
    "user": "jaggu",
    "items": "Milk",
    "quantity": 99
}
value = json.dumps(order).encode('utf-8')
producer.produce(
    topic='orders',
    value=value,
    callback=delivery_report
)
producer.flush()
