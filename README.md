# PoC AMQP 0-9-1: Redes Avanzadas (IIoT Demo)

Prueba de concepto (PoC) reproducible de **AMQP 0-9-1** implementada con **Python**, **RabbitMQ** y **Docker**, diseñada para demostraciones académicas en la materia **Redes Avanzadas**.

---

## 🛠️ Arquitectura y Topología Lógica

```text
               +-------------------------------------------------------+
               |                  RABBITMQ BROKER                      |
               |                                                       |
               |                 [Topic Exchange]                      |
[ PRODUCER ] --|--> Basic.Publish('industria.eventos', RK)             |
               |                           |                           |
               |         +-----------------+-----------------+         |
               |         | Binding: '#.metric'               | Binding: '#.critical'
               |         v                                   v         |
               |   [( Cola Telemetría )]               [( Cola Alarmas )]
               |   - linea1.prensa.metric (x2)         - linea1.prensa.critical (x1)
               +---------|-----------------------------------|---------+
                         | Basic.Deliver (ráfaga)            | Basic.Deliver (QoS prefetch=1)
                         v (no-ack: True)                    v (no-ack: False)
                [ CONSUMER TELEMETRÍA ]             [ CONSUMER CRÍTICO ]
                - Sin Basic.Ack (Alto throughput)   - Espera 4s + Basic.Ack explícito
```

---

## 🛠️ FASE 0: Preparación antes de empezar a exponer

1. **Terminal** abierta en la carpeta del proyecto:
   ```bash
   cd /Users/karinacarlo/Desktop/PoC-amqp-tar
   source venv/bin/activate
   docker compose up -d
   ```
   *(Si estuviste haciendo pruebas antes, tirá `docker compose restart rabbitmq` para dejar las colas limpias en 0).*
2. **Navegador** abierto en [http://localhost:15672/#/queues](http://localhost:15672/#/queues) (Usuario: `guest`, Contraseña: `guest`).
   * Arriba a la derecha cambiá *"Do not refresh"* a **"Refresh every 5 seconds"**.
3. **Wireshark** abierto:
   * Elegí la interfaz **`Loopback: lo0`** y dejalo capturando.
   * En la barra de filtro escribí **`amqp`** y apretá Enter.

---

## 🎬 PASO 1: Emitir los mensajes y ver el enrutamiento (30 seg)

1. **En la Terminal**, ejecutá el productor:
   ```bash
   python producer.py
   ```
2. **En el Navegador (RabbitMQ UI)**, mostrá la tabla de colas:
   * `cola_telemetria_historica` $\to$ **Ready: 2**
   * `cola_alarmas_criticas` $\to$ **Ready: 1**
3. **Qué explicar**:
   > *"El productor envió los 3 eventos a un único Topic Exchange (`industria.eventos`). El broker inspeccionó las Routing Keys (`#.metric` vs `#.critical`) y bifurcó los mensajes en dos colas distintas. Además, como todavía no prendimos ningún consumidor, vemos el desacoplamiento temporal: los mensajes quedan retenidos de forma segura en el broker."*

---

## ⚡ PASO 2: Ejecutar los dos Consumidores (40 seg)

1. **Consumidor Crítico (`auto_ack=False` + QoS)**:
   ```bash
   python consumer_critical.py
   ```
   * Recibe la alarma crítica, espera **4 segundos** (simulando escritura transaccional) e imprime:
     `[✓] Procesamiento confirmado. Trama Basic.Ack(tag=1) enviada al broker.`
   * Apretá **`Ctrl + C`** para salir.
2. **Consumidor de Telemetría (`auto_ack=True`)**:
   ```bash
   python consumer_telemetry.py
   ```
   * Consume los **2 mensajes de sensores al instante** sin demoras.
   * Apretá **`Ctrl + C`** para salir.
3. **En el Navegador (RabbitMQ UI)**:
   * Mostrá que **ambas colas quedaron en 0 mensajes**.

---

## 🔍 PASO 3: Análisis de Tramas en Wireshark (1 min 30 seg)

Tocá el cuadrado rojo (**Stop**) en Wireshark. En una sola captura tenés todo el ciclo de vida. Recorré estos **6 puntos** de arriba hacia abajo:

### 1. Handshake del Protocolo (`Protocol-Header 0-9-1`)
* Hacé clic en el primer paquete AMQP.
* **Qué decir**: *"El cliente inicia enviando los 8 bytes de cabecera literal `AMQP 0-9-1` (`41 4d 51 50 00 00 09 01`)."*

### 2. Multiplexación sobre el mismo Socket TCP (`Channel: 0` vs `Channel: 1`)
* Hacé clic en `Connection.Start` y luego más abajo en `Channel.Open`.
* **Qué decir**: *"Si miramos la capa TCP arriba, todos los paquetes del productor usan el mismo puerto origen contra el 5672 (un único socket TCP). Adentro de esa única conexión TCP, AMQP multiplexa canales virtuales: usa el `Channel: 0` exclusivamente para el control de la conexión y abre el `Channel: 1` para enviar los datos."*

### 3. Las 3 Tramas de una Publicación (`Basic.Publish Content-Header Content-Body`)
* Hacé clic en uno de los paquetes de publicación y desplegalo en el panel izquierdo:
* **Qué decir**: *"En AMQP, publicar 1 mensaje se divide en 3 tramas (Frames) que viajan concatenadas dentro del mismo segmento TCP:*
  1. ***Method Frame (`Type: 1` - `Basic.Publish`)**: Indica el Exchange (`industria.eventos`) y la Routing Key.*
  2. ***Header Frame (`Type: 2` - `Content-Header`)**: Lleva las propiedades del mensaje; acá vemos `Delivery-Mode: 2 (Persistent)` para que se guarde en disco y `application/json`.*
  3. ***Body Frame (`Type: 3` - `Content-Body`)**: Lleva el JSON crudo del sensor.*
  * *Y al final de cada trama vemos el byte delimitador obligatorio **`0xCE`** para detectar errores de framing."*

### 4. Consumidor Crítico: Control de Flujo (`Basic.Qos`) y Suscripción (`Basic.Consume`)
* Bajá hasta donde se conectó el primer consumidor:
* **Qué decir**:
  * *"Primero envía `Basic.Qos` con `Prefetch-Count: 1` para que el broker no le mande más de un mensaje a la vez."*
  * *"En `Basic.Consume` vemos el flag **`no-ack: False`**, avisándole al broker que requiere confirmación manual."*

### 5. Consumidor Crítico: `Basic.Deliver` y los 4 segundos hasta el `Basic.Ack`
* Señalá el paquete `Basic.Deliver` y el paquete `Basic.Ack` justo debajo:
* **Qué decir**: *"El broker entrega la alarma con `Basic.Deliver`. Si miramos la columna **Time**, recién **4 segundos después** aparece la trama **`Basic.Ack` (`Delivery-Tag: 1`)** enviada por el consumidor. Durante esos 4 segundos el mensaje estuvo retenido como `Unacked` en el broker: si el consumidor se caía antes de mandar este `Basic.Ack`, RabbitMQ detecta el cierre del socket TCP y vuelve a encolar la alarma sin perder datos."*

### 6. Consumidor de Telemetría: Modo Ráfaga (`no-ack: True` y sin `Basic.Ack`)
* Bajá hasta los últimos paquetes (el segundo consumidor):
* **Qué decir**: *"En cambio, en el consumidor de telemetría vemos dos diferencias clave en la red:*
  1. *En `Basic.Consume` el flag dice **`no-ack: True`** (confirmación automática).*
  2. *No hay `Basic.Qos` ni paquetes `Basic.Ack` de vuelta: el broker dispara los dos `Basic.Deliver` juntos en ráfaga y los borra inmediatamente, ahorrando paquetes en la red para maximizar el throughput."*
