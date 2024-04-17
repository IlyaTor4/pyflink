from kafka import KafkaConsumer

bootstrap_servers = ['0.0.0.0:9092']  # python local, broker in container
# bootstrap_servers = ['broker:29092']  # python in container, broker in container
topic = 'picks'

print('creating consumer')
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-test'
)
print('consuming...')
for message in consumer:
    for m in message:
        print(m)
