import json
import time
import random
from kafka import KafkaProducer

# Conexión con el servidor Kafka
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("🚀 Sensores de SmartManuTech transmitiendo datos...")

while True:
    datos = {
        "sensor_id": "Prensa_Hidraulica_01",
        "temperatura": round(random.uniform(60, 115), 2),
        "vibracion": round(random.uniform(1, 10), 2),
        "energia_kw": round(random.uniform(20, 55), 2)
    }
    producer.send('telemetria', value=datos)
    print(f"📡 Enviando a Kafka: {datos}")
    time.sleep(1) # Envía un dato por segundo