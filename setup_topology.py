import pika

def main():
    print("[*] Conectando a RabbitMQ para aprovisionamiento de topología (IaC)...")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', port=5672, virtual_host='/')
    )
    channel = connection.channel()

    exchange_name = 'industria.eventos'
    queue_telemetria = 'cola_telemetria_historica'
    queue_alarmas = 'cola_alarmas_criticas'

    # Declaración idempotente de componentes (Infraestructura como Código / Fase 0)
    print(" [*] Declarando Exchange de tipo Topic...")
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    print(" [*] Declarando Colas Durables...")
    channel.queue_declare(queue=queue_telemetria, durable=True)
    channel.queue_declare(queue=queue_alarmas, durable=True)

    print(" [*] Creando Bindings por patrón de routing key...")
    channel.queue_bind(exchange=exchange_name, queue=queue_telemetria, routing_key='#.metric')
    channel.queue_bind(exchange=exchange_name, queue=queue_alarmas, routing_key='#.critical')

    connection.close()
    print("[✓] Topología aprovisionada exitosamente en el broker.")
    print("    - Exchange : industria.eventos (Topic)")
    print("    - Colas    : cola_telemetria_historica, cola_alarmas_criticas")
    print("    - Bindings : #.metric, #.critical\n")

if __name__ == '__main__':
    main()
