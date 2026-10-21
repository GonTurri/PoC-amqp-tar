import json
import time
import pika

def main():
    # 1. Conexión TCP al puerto IANA 5672 y canal lógico 1
    # Usamos 127.0.0.1 para captura limpia en la interfaz loopback (lo0 en macOS / lo en Linux)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', port=5672, virtual_host='/')
    )
    channel = connection.channel()

    exchange_name = 'industria.eventos'
    queue_telemetria = 'cola_telemetria_historica'
    queue_alarmas = 'cola_alarmas_criticas'

    # 2. Declaración asertiva de componentes (Idempotencia del protocolo)
    # durable=True indica persistencia de metadatos ante reinicios del broker
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
    channel.queue_declare(queue=queue_telemetria, durable=True)
    channel.queue_declare(queue=queue_alarmas, durable=True)

    # 3. Cableado dinámico (Bindings por patrón de routing key)
    # NOTA DE REDES AVANZADAS:
    # En AMQP 0-9-1 Topic Exchange:
    #  - '*' sustituye EXACTAMENTE UNA palabra entre puntos.
    #  - '#' sustituye CERO O MÁS palabras entre puntos.
    # Como la clave es 'linea1.prensa.metric' (3 palabras), usar '*.metric' NO empareja (espera 2 palabras).
    # Usamos '#.metric' y '#.critical' (o '*.*.metric' / '*.*.critical') para emparejar dinámicamente.
    channel.queue_bind(exchange=exchange_name, queue=queue_telemetria, routing_key='#.metric')
    channel.queue_bind(exchange=exchange_name, queue=queue_alarmas, routing_key='#.critical')

    # 4. Carga de eventos industriales
    eventos = [
        ("linea1.prensa.metric", {"sensor": "presion_hidraulica", "valor_bar": 180.2, "status": "NORMAL"}),
        ("linea1.prensa.metric", {"sensor": "temperatura_aceite", "valor_c": 62.4, "status": "NORMAL"}),
        ("linea1.prensa.critical", {"sensor": "temperatura_aceite", "valor_c": 115.8, "alerta": "PARADA_EMERGENCIA_SOBRECALENTAMIENTO"})
    ]

    print("[*] Publicando eventos industriales con persistencia en disco (delivery_mode=2)...")
    for routing_key, payload in eventos:
        # En el wire, cada publicación emite 3 tramas AMQP:
        # 1. Method Frame (Basic.Publish)
        # 2. Header Frame (Content Header con properties y body size)
        # 3. Body Frame (Content Body con los bytes de la carga útil)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente (almacenamiento no volátil)
                content_type='application/json'
            )
        )
        print(f" [->] Enviado: '{routing_key}' -> Payload: {payload['sensor']}")
        time.sleep(0.4)

    connection.close()
    print("[✓] Lote emitido y conexión AMQP cerrada exitosamente.")

if __name__ == '__main__':
    main()
