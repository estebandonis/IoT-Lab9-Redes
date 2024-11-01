import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

bootstrap_servers = '164.92.76.15:9092'
topic = '21610'

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    topic,
    group_id='grupo_consumidores',
    bootstrap_servers=[bootstrap_servers]
)

# Bucle principal del consumidor para escuchar mensajes y procesarlos
for mensaje in consumer:
    if mensaje.value is not None:
        payload = json.loads(mensaje.value)
        print(payload)
    else:
        print("Mensaje recibido sin payload.")