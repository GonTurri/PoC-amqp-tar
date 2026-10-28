import json
import time
import pika

def main():
    # 1. Conexión TCP al broker AMQP (puerto IANA 5672)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', port=5672, virtual_host='/')
    )
    channel = connection.channel()

    # NOTA DE REDES AVANZADAS (DESACOPLAMIENTO ESPACIAL):
    # El productor ÚNICAMENTE conoce el Exchange ('industria.eventos') y su Routing Key.
    # Desconoce por completo cuántas colas existen, sus nombres o qué consumidores están conectados.
    # La topología fue aprovisionada previamente en la Fase 0 (setup_topology.py / IaC).
    exchange_name = 'industria.eventos'

    # 2. Carga de eventos industriales
    eventos = [
        ("linea1.prensa.metric", {"sensor": "presion_hidraulica", "valor_bar": 180.2, "status": "NORMAL"}),
        ("linea1.prensa.metric", {"sensor": "temperatura_aceite", "valor_c": 62.4, "status": "NORMAL"}),
        ("linea1.prensa.critical", {"sensor": "temperatura_aceite", "valor_c": 115.8, "alerta": "PARADA_EMERGENCIA_SOBRECALENTAMIENTO"})
    ]

    print("[*] Publicando eventos industriales desacoplados (delivery_mode=2)...")
    for routing_key, payload in eventos:
        # En el wire, cada publicación emite la tríada de tramas AMQP:
        # 1. Method Frame (Basic.Publish)
        # 2. Header Frame (Content Header con properties y body size)
        # 3. Body Frame (Content Body con los bytes de la carga útil) + [0xCE]
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente en almacenamiento no volátil
                content_type='application/json'
            )
        )
        print(f" [->] Enviado al Exchange: '{routing_key}' -> Payload: {payload['sensor']}")
        time.sleep(0.3)

    connection.close()
    print("[✓] Lote emitido y conexión AMQP cerrada exitosamente.")

if __name__ == '__main__':
    main()
