# PoC AMQP 0-9-1: Redes Avanzadas (IIoT Demo)

Prueba de concepto (PoC) reproducible de **AMQP 0-9-1** implementada con **Python**, **RabbitMQ** y **Docker**, diseñada para demostraciones académicas en la materia **Redes Avanzadas**.

---

## 🎯 Objetivos de la Demostración (≤ 3 Minutos)

1. **Inspección Wire-Level (Wireshark)**:
   - Handshake del protocolo AMQP (`'AMQP' 0 0 9 1`).
   - Multiplexación lógica de canales (Canal 0 para control de conexión, Canal 1 para aplicación).
   - Secuencia de publicación en 3 tramas: **Method Frame (`Basic.Publish`)**, **Content Header Frame**, y **Content Body Frame**.
   - Octeto delimitador de sincronización de trama: **`0xCE`** (decimal 206).
2. **Enrutamiento Dinámico (Topic Exchange)**:
   - Demostrar cómo el broker enruta mensajes a colas según jerarquías con comodines (`#` vs `*`).
3. **Tolerancia a Fallos y Control de Flujo**:
   - Control de flujo con **QoS** (`prefetch_count=1`).
   - Acuses de recibo manuales (`Basic.Ack`).
   - Simulación de caída del nodo (`Ctrl+C`): detección de caída de conexión y reencolado automático (*Unacked* $\to$ *Ready*).
   - Detección de reentrega en la cabecera del protocolo (`redelivered=True`).

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
                         | Basic.Deliver                     | Basic.Deliver
                         v (auto_ack=True)                   v (prefetch=1, manual ack)
                [ CONSUMER TELEMETRÍA ]             [ CONSUMER CRÍTICO ]
                - Fire & Forget                     - Espera 4s (simulación)
                - Alto rendimiento                  - Basic.Ack explícito
```

---

## 🚀 Requisitos y Puesta en Marcha

### 1. Levantar el Broker RabbitMQ

```bash
docker compose up -d
```

> **Verificación**: La interfaz web estará disponible en [http://localhost:15672](http://localhost:15672) (Usuario: `guest`, Contraseña: `guest`).

### 2. Configurar Entorno Python

```bash
python3 -m venv venv
source venv/bin/activate       # En Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## ⏱️ Guión de la Presentación en Vivo (Paso a Paso en 3 Minutos)

### Minuto 0:00 - 1:00 | Captura en Wireshark y Emisión de Tráfico

1. Abrir **Wireshark**.
2. Seleccionar la interfaz de captura:
   - **macOS**: `Loopback: lo0`
   - **Linux**: `lo`
   - **Windows**: `Adapter for loopback traffic capture` (Npcap)
3. Establecer el filtro de visualización:
   ```text
   amqp
   ```
4. En una terminal, ejecutar el emisor:
   ```bash
   python producer.py
   ```
5. En Wireshark, detener la captura y destacar ante los profesores:
   - **Protocol Header (Trama 8 bytes)**: `AMQP 0-9-1` (Hex: `41 4d 51 50 00 00 09 01`).
   - **Negociación de Conexión**: `Connection.Start` $\to$ `Connection.Start-Ok` $\to$ `Connection.Tune` $\to$ `Connection.Open`.
   - **Apertura de Canal Lógico**: `Channel.Open` en el Canal 1.
   - **Trilogía de Publicación** (seleccionar un mensaje enviado):
     - **Trama 1 (Method Frame - Type 1)**: `Basic.Publish` indicando Exchange `industria.eventos` y Routing Key `linea1.prensa.metric`.
     - **Trama 2 (Header Frame - Type 2)**: `Content Header` con `delivery_mode: 2` (Persistente) y `content_type: application/json`.
     - **Trama 3 (Body Frame - Type 3)**: `Content Body` con la carga JSON en texto claro.
   - **Delimitador de Fin de Trama**: Mostrar en el panel de bytes hexadecimales que cada trama finaliza rigurosamente con el byte **`0xCE`**.

---

### Minuto 1:00 - 1:45 | Verificación en RabbitMQ Management

1. Abrir en el navegador [http://localhost:15672/#/queues](http://localhost:15672/#/queues).
2. Demostrar el desacoplamiento y el Topic Exchange:
   - Sin haber iniciado ningún consumidor, el broker evaluó las routing keys de los mensajes:
     - `cola_telemetria_historica` tiene **2** mensajes en estado **Ready**.
     - `cola_alarmas_criticas` tiene **1** mensaje en estado **Ready**.

---

### Minuto 1:45 - 2:45 | Tolerancia a Fallos, QoS y Reentrega Automática

1. En la terminal, iniciar el consumidor crítico:
   ```bash
   python consumer_critical.py
   ```
2. Observar la consola:
   - El consumidor recibe la alarma:
     `[!] ALERTA CRÍTICA RECIBIDA | Delivery Tag: 1 | Redelivered: False`
   - Entra en la ventana de espera de 4 segundos:
     `>>> ¡PRESIONA CTRL+C AHORA SI DESEAS SIMULAR LA CAÍDA DEL NODO! <<<`
3. En la interfaz web de RabbitMQ:
   - La cola pasa instantáneamente de `Ready: 1` a `Unacked: 1`. Esto evidencia el control de flujo **QoS** (`prefetch_count=1`).
4. **Simular la caída**: Presionar **`Ctrl+C`** en la terminal.
   - El proceso muere abruptamente sin emitir `Basic.Ack`.
   - La conexión TCP se cierra.
   - En RabbitMQ Management: La cola vuelve automáticamente a **`Ready: 1, Unacked: 0`**. El broker reencoló el mensaje al detectar la pérdida del canal, garantizando cero pérdida de datos.
5. **Comprobación de Reentrega**: Volver a ejecutar el consumidor:
   ```bash
   python consumer_critical.py
   ```
   - El consumidor vuelve a recibir el mensaje, pero ahora con:
     `Redelivered: True (¡REENTREGA DETECTADA TRAS FALLA!)`
   - Dejar correr los 4 segundos: se emite `Basic.Ack(tag=1)`.
   - En la UI de RabbitMQ, los mensajes de `cola_alarmas_criticas` bajan definitivamente a **0**.
6. **(Opcional) Consumo de Telemetría**:
   - Si se desea vaciar la cola de telemetría o los evaluadores lo consultan:
     ```bash
     python consumer_telemetry.py
     ```
   - Procesa los eventos instantáneamente demostrando el modo de alta frecuencia sin confirmación manual (`auto_ack=True`).

---

## 🔍 Análisis Teórico para Preguntas de Examen / Profesores

### 1. Comparativa de Consumo: `auto_ack=False` vs `auto_ack=True`
| Característica | Consumidor Crítico (`consumer_critical.py`) | Consumidor Telemetría (`consumer_telemetry.py`) |
| :--- | :--- | :--- |
| **Modo de Acuse** | `auto_ack=False` (Manual a nivel aplicación) | `auto_ack=True` (Automático a nivel transporte) |
| **Garantía** | *At-Least-Once* (Cero pérdida de datos) | *At-Most-Once* (Riesgo de pérdida ante caídas) |
| **Throughput / Latencia** | Moderado (espera confirmación y ventana QoS) | Máximo (sin sobrecoste de tramas `Basic.Ack`) |
| **Caso de Uso** | Alarmas de seguridad, transacciones críticas | Métricas periódicas de sensores IIoT |

### 2. ¿Por qué `#` y no `*` en el Topic Exchange?
En el estándar AMQP 0-9-1:
- `*` (asterisco) sustituye **estrictamente una sola palabra** delimitada por puntos.
- `#` (numeral/hash) sustituye **cero, una o múltiples palabras**.
Dado que la clave de enrutamiento es `linea1.prensa.metric` (3 palabras):
- `*.metric` espera 2 palabras $\implies$ **No coincide** (el mensaje se descartaría).
- `*.*.metric` o `#.metric` $\implies$ **Coincide**. Usar `#.metric` permite que cualquier equipo industrial en cualquier nivel jerárquico envíe métricas a la cola histórica.

### 2. Estructura Binaria de la Cabecera de Trama AMQP (7 Bytes)
| Offset (Bytes) | Campo | Tamaño | Descripción |
| :--- | :--- | :--- | :--- |
| `0` | `Type` | 1 byte | 1=Method, 2=Header, 3=Body, 4=Heartbeat |
| `1 - 2` | `Channel` | 2 bytes | Identificador del canal lógico (0=Global, 1..N=App) |
| `3 - 6` | `Payload Size` | 4 bytes | Longitud en bytes del cuerpo de la trama |
| `7 ... N+6` | `Payload` | N bytes | Contenido del método, propiedades o datos |
| `N+7` | `Frame End` | 1 byte | Constante obligatoria **`0xCE`** (206 dec) |

### 3. Diferencia entre AMQP y MQTT / HTTP
- **Frente a HTTP/REST**: AMQP es full-duplex y asíncrono sobre una única conexión TCP multiplexada en canales ligeros. No incurre en el sobrecoste de establecer conexiones HTTP continuas ni cabeceras repetitivas.
- **Frente a MQTT**: MQTT usa un broker de publicación/suscripción básico sin topología desacoplada. En AMQP, los publicadores nunca publican directo a una cola; publican a un **Exchange**, lo que permite lógica de enrutamiento programable (direct, fanout, topic, headers) y confirmaciones transaccionales rigurosas (`Basic.Ack`, `Basic.Nack`, `Basic.Reject`).
