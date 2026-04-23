from kafka import KafkaConsumer
from const import *
import sys

try:
    topic = sys.argv[1]
except:
    print('Usage: python3 consumer <topic_name>')
    exit(1)

consumer = KafkaConsumer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest'
)

consumer.subscribe([topic])

print(f'Consumer escutando o topico "{topic}"...')

for msg in consumer:
    print('Recebido: ' + msg.value.decode())