import json
import pika

def callback(ch, method, properties, body):
    evento = json.loads(body.decode())
    print("\n" + "="*60)
    print(f"[!] ALERTA CRÍTICA RECIBIDA")
    print(f"    - Delivery Tag : {method.delivery_tag}")
    print(f"    - Redelivered  : {method.redelivered} {'(¡REENTREGA TRAS CAÍDA PREVIA!)' if method.redelivered else '(Primera entrega)'}")
    print(f"    - Routing Key  : {method.routing_key}")
    print(f"    - Exchange     : {method.exchange}")
    print(f"    - Payload      : {evento}")
    print("="*60)
    print("[⏸️] Estado en RabbitMQ: UNACKED (1 mensaje retenido en el broker).")
    print("[*] Acciones disponibles:")
    print("    -> [ENTER]  : Confirmar procesamiento y enviar Basic.Ack al broker.")
    print("    -> [CTRL+C] : Simular caída del proceso (RabbitMQ lo devuelve a 'Ready: 1').")
    
    input("\nPresioná [ENTER] para emitir Basic.Ack o [CTRL+C] para forzar caída... ")
    
    # Confirmación explícita de procesamiento a nivel de capa de aplicación
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"\n[✓] Procesamiento confirmado. Trama Basic.Ack(tag={method.delivery_tag}) enviada al broker.")
    print("="*60)

def main():
    # heartbeat=600 (10 minutos) para permitir pausar y explicar en la UI sin timeout de latidos
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', port=5672, virtual_host='/', heartbeat=600)
    )
    channel = connection.channel()

    queue_name = 'cola_alarmas_criticas'
    # Declaración idempotente por si se ejecuta de forma independiente
    channel.queue_declare(queue=queue_name, durable=True)

    # Control de flujo (QoS - Backpressure):
    # prefetch_count=1 restringe al broker a no despachar nuevos mensajes a este consumidor
    # hasta que el mensaje actual sea explicitamente confirmado (Acked).
    channel.basic_qos(prefetch_count=1)

    # Consumo con confirmación manual (auto_ack=False)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    print(f"[*] Consumidor crítico activo escuchando en '{queue_name}'...")
    print("[*] Esperando mensajes. Presione CTRL+C durante el procesamiento para forzar caída.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n" + "-"*60)
        print("[!] Proceso interrumpido deliberadamente antes de emitir Basic.Ack.")
        print("[!] Al cerrarse el canal/conexión TCP sin Ack, RabbitMQ reencola el mensaje")
        print("    inmediatamente pasando de estado 'Unacked' a 'Ready' sin pérdida de datos.")
        print("-" * 60)
        try:
            if connection.is_open:
                connection.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
