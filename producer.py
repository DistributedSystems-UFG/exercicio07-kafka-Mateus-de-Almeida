from kafka import KafkaProducer
from const import *
import sys

try:
    topic = sys.argv[1]
except:
    print('Usage: python3 producer <topic_name>')
    exit(1)

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

for i in range(100):
    msg = 'Mensagem ' + str(i) + ' para o topico ' + topic
    print('Enviando: ' + msg)
    producer.send(topic, value=msg.encode())

producer.flush()
print('Todas as mensagens enviadas!')