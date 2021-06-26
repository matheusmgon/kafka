from kafka import KafkaConsumer
from json import loads

kafka_topic = input("Digite o nome do tópico: \n")

consumer = KafkaConsumer(
    kafka_topic,
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

for message in consumer:
    message = message.value
    print(message)