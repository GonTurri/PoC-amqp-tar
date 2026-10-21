import json
import pika

def callback(ch, method, properties, body):
    evento = json.loads(body.decode())
    valor = evento.get('valor_bar', evento.get('valor_c'))
    unidad = "bar" if 'valor_bar' in evento else "°C"
    print(f" [📊 Telemetría] Sensor: {evento.get('sensor'):<20} | Valor: {valor:>5} {unidad} | Estado: {evento.get('status')}")

def main():
    # Conexión al broker AMQP
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1', port=5672, virtual_host='/')
    )
    channel = connection.channel()

    queue_name = 'cola_telemetria_historica'
    channel.queue_declare(queue=queue_name, durable=True)

    # NOTA DE REDES AVANZADAS:
    # auto_ack=True (Confirmación automática a nivel de transporte):
    # En cuanto el broker despacha la trama al socket TCP del consumidor,
    # la considera entregada y la borra del buffer.
    # Es el modo ideal para streaming IIoT / Big Data de alta frecuencia (alto throughput)
    # donde prima la baja latencia y se tolera la pérdida eventual de muestras.
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print(f"[*] Worker de telemetría histórica escuchando en '{queue_name}'...")
    print("[*] Configuración: auto_ack=True (Alta velocidad / Sin ACKs manuales)")
    print("[*] Presione CTRL+C para salir.\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[*] Consumidor de telemetría detenido.")
        try:
            if connection.is_open:
                connection.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
