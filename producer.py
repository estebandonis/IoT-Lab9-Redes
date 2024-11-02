import json
import time
import random
from confluent_kafka import Producer

# Configuración del productor de Kafka
bootstrap_servers = '164.92.76.15:9092'
topic = '21611'

temperatura_media = 50.0
humedad_media = 50
varianza_temperatura = 10.0
varianza_humedad = 10

conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

# Función para generar datos meteorológicos
def generar_datos():
    temperatura = round(random.uniform(temperatura_media - varianza_temperatura, temperatura_media + varianza_temperatura), 1)
    humedad = random.randint(humedad_media - varianza_humedad, humedad_media + varianza_humedad)
    direccion_viento = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])
    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }

# Función de reporte de entrega para el productor
def delivery_report(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

# Función para enviar datos
def enviar_datos():
    while True:
        datos = generar_datos()
        datos_serializados = json.dumps(datos).encode('utf-8')
        print(datos)
        if datos is not None:
            producer.produce(topic, key="sensor", value=datos_serializados, callback=delivery_report)
            producer.flush()
        else:
            print("No se pudo codificar el mensaje.")
        
        time.sleep(30)

if __name__ == '__main__':
    enviar_datos()
