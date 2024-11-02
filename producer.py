import json
import time
import random
from confluent_kafka import Producer

# Configuración del productor de Kafka
bootstrap_servers = '164.92.76.15:9092'
topic = '21612'

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

def encode(temperatura, humedad, direccion_viento):
    direcciones = {"N": 0, "NW": 1, "W": 2, "SW": 3, "S": 4, "SE": 5, "E": 6, "NE": 7}
    
    # Asegurar que la temperatura codificada esté en el rango 0 a 16383
    temp_ajustada = max(min(temperatura, 50), -50)
    temp_codificada = int((temp_ajustada + 50) * 163.83)  # Ajuste para que quepa en 14 bits

    dir_viento_codificada = direcciones[direccion_viento]
    payload = (temp_codificada << 10) | (humedad << 3) | dir_viento_codificada

    # Imprimir detalles de codificación
    print(f"Codificando: Temperatura={temperatura}, Temp. Codificada={temp_codificada}, "
          f"Humedad={humedad}, Dirección Viento={direccion_viento} (Codificada={dir_viento_codificada}), "
          f"Payload (antes de bytes)={payload}")

    try:
        return payload.to_bytes(3, byteorder='big')
    except OverflowError:
        print(f"Error: valor de payload demasiado grande para convertir: {payload}")
        return None

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
        datos_codificado = encode(datos['temperatura'], datos['humedad'], datos['direccion_viento'])

        print(f"Enviando datos: {datos_codificado}")

        if datos is not None:
            producer.produce(topic, key="sensor", value=datos_codificado, callback=delivery_report)
            producer.flush()
        else:
            print("No se pudo codificar el mensaje.")
        
        time.sleep(5)

if __name__ == '__main__':
    enviar_datos()
