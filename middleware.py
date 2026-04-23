from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in  = sys.argv[1]  # ex: topic1
    topic_out = sys.argv[2]  # ex: topic2
except:
    print('Usage: python3 middleware <topic_in> <topic_out>')
    exit(1)

# Consome mensagens do topic1
consumer = KafkaConsumer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest'  # lê desde o início do tópico
)

# Produz mensagens no topic2
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic_in])

print(f"Middleware escutando {topic_in} e repassando para {topic_out}")

for msg in consumer:
    original = msg.value.decode()
    transformada = '<transform> ' + original.upper()  # transforma a mensagem
    print(f'Recebido: {original}')
    print(f'Repassando: {transformada}\n')
    producer.send(topic_out, value=transformada.encode())
    producer.flush()