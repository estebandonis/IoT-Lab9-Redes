import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

bootstrap_servers = '164.92.76.15:9092'
topic = '21611'

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

# Bucle principal del consumidor para escuchar mensajes y procesarlos
for mensaje in consumer:
    if mensaje.value is not None:
        payload = json.loads(mensaje.value)
        all_temp.append(payload['temperatura'])
        all_hume.append(payload['humedad'])
        all_wind.append(direccion_viento_a_numero(payload['direccion_viento']))
        plotAllData(all_temp, all_hume, all_wind)
    else:
        print("Mensaje recibido sin payload.")