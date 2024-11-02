import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

bootstrap_servers = '164.92.76.15:9092'
topic = '21612'

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    topic,
    group_id='grupo_consumidores',
    bootstrap_servers=[bootstrap_servers]
)

all_temp = []
all_hume = []
all_wind = []

# Inicialización del gráfico
plt.ion()
fig, axs = plt.subplots(3, 1, figsize=(10, 6))
line_temp, = axs[0].plot([], [], label='Temperatura')
line_hume, = axs[1].plot([], [], label='Humedad')
line_wind, = axs[2].plot([], [], label='Dirección Viento')
for ax in axs:
    ax.legend()
    ax.set_autoscaley_on(True)
    ax.set_autoscalex_on(True)

# Función para actualizar los datos del gráfico
def plotAllData(temperaturas, humedades, direcciones_viento):
    line_temp.set_data(range(len(temperaturas)), temperaturas)
    line_hume.set_data(range(len(humedades)), humedades)
    line_wind.set_data(range(len(direcciones_viento)), direcciones_viento)
    for ax in axs:
        ax.relim()
        ax.autoscale_view()
    fig.canvas.draw()
    fig.canvas.flush_events()

def direccion_viento_a_numero(direccion_viento):
    direcciones = {"N": 0, "NE": 1, "E": 2, "SE": 3, "S": 4, "SW": 5, "W": 6, "NW": 7}
    return direcciones.get(direccion_viento, -1)

def decode(payload_bytes):
    payload = int.from_bytes(payload_bytes, byteorder='big')
    temp_codificada = (payload >> 10) & 0x3FFF  # 14 bits para la temperatura
    temperatura = (temp_codificada / 163.84) - 50
    humedad = (payload >> 3) & 0x7F  # 7 bits para la humedad
    direccion_viento = payload & 0x07  # 3 bits para la dirección del viento
    direcciones = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]
    direccion_viento_str = direcciones[direccion_viento]
    print(f"Decodificación: Temp={temperatura}, Hum={humedad}, Dir={direccion_viento_str}")
    return temperatura, humedad, direccion_viento_str

# Bucle principal del consumidor para escuchar mensajes y procesarlos
for mensaje in consumer:
    if mensaje.value is not None:
        payload = decode(mensaje.value)
        all_temp.append(payload[0])
        all_hume.append(payload[1])
        all_wind.append(direccion_viento_a_numero(payload[2]))
        plotAllData(all_temp, all_hume, all_wind)
    else:
        print("Mensaje recibido sin payload.")