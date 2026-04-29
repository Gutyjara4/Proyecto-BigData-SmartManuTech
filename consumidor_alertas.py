import json
from kafka import KafkaConsumer

# Conexión al bus de datos de Kafka
consumer = KafkaConsumer(
    'telemetria',
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🧠 Analizador de Datos iniciado. Esperando señales...")

for mensaje in consumer:
    d = mensaje.value
    t = d['temperatura']
    v = d['vibracion']
    e = d['energia_kw']
    
    # --- LÓGICA DE LAS 5 ALERTAS (Para tu PDF) ---
    if t > 105:
        print(f"🔴 [ALERTA 1] PELIGRO: Temperatura Crítica ({t}°C).")
    elif t > 90:
        print(f"🟠 [ALERTA 2] AVISO: Temperatura elevada ({t}°C).")
        
    if v > 8.5:
        print(f"⚠️ [ALERTA 3] VIBRACIÓN: Desgaste detectado ({v} Hz).")
        
    if e > 48:
        print(f"⚡ [ALERTA 4] CONSUMO: Pico de energía detectado ({e} kW).")
        
    if t > 95 and v > 7:
        print(f"🚨 [ALERTA 5] EMERGENCIA: Riesgo de rotura inminente.")