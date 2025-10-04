import json

from confluent_kafka import Consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["orders"])
print("🟢 Consumer is subscribed to topic 'orders' and running")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:",msg.error())
            continue
        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(order)
except KeyboardInterrupt:
    print("Closing consumer")
finally:
    consumer.close()

