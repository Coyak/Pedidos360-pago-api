# Pedidos360 - Microservicio de Pagos & Antifraude (ms-pago)

Microservicio transaccional desarrollado en **Spring Boot 3.4 (Java 21)** para la **Evaluación 3** de Desarrollo Cloud. Gestiona el ciclo de vida completo de pagos electrónicos con **Transbank Webpay Plus (SDK 6.x Oficial)** y el **Motor Antifraude de Velocidad** con bloqueo preventivo **HTTP 429 Too Many Requests**.

---

## 🎯 Alcance y Funcionalidades Principales

1. **Integración Oficial con Transbank Webpay Plus (REST SDK 6.x):**
   - Inicia transacciones contra el entorno de integración oficial (`https://webpay3gint.transbank.cl`).
   - Código de comercio de integración: `597055555532`.
   - API Key oficial de 64 caracteres: `579B532A7440BB0C9079DED94D31EA1615BACEB56610332264630D42D0A36B1C`.
   - Genera tokens de transacción criptográficos (`token_ws`) y procesa el commit en `/callback`.
2. **Motor Antifraude de Velocidad (Regla Crítica EV3):**
   - Evalúa ráfagas de transacciones por cliente: **más de 3 intentos en menos de 10 segundos**.
   - Ante detección de ataque de fuerza bruta o velocidad sospechosa, responde inmediatamente con **HTTP 429 Too Many Requests** y activa un bloqueo temporal de 30 minutos.
   - Publica alertas de fraude hacia Apache Kafka en el tópico `topic-alertas-fraude`.
3. **Persistencia Transaccional:**
   - Registra cada transacción (`INICIADO`, `AUTHORIZED`, `REJECTED`, `FRAUDE_BLOQUEADO`) en la base de datos relacional MySQL 8.0 (`pagos_db`).
4. **Despliegue 100% Remoto (Regla "CERO LOCALHOST"):**
   - Configuración desacoplada en `application.yml` mediante variables de entorno: `SERVER_PORT`, `SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `TRANSBANK_RETURN_URL`.

---

## 📡 Endpoints de la API (Puerto 8085)

| Método | Endpoint | Descripción | Respuestas |
|---|---|---|---|
| `GET` | `/api/pagos/status` | Healthcheck y estado del microservicio | `200 OK` (JSON) |
| `POST` | `/api/pagos/iniciar` | Inicia pago y evalúa regla antifraude | `200 OK` (`url` + `token`) o `429 Too Many Requests` |
| `POST` | `/api/pagos/commit?token_ws={token}` | Confirma transacción con Transbank | `200 OK` (Voucher electrónico) o `400` / `500` |
| `GET` | `/api/pagos/estado-cliente/{clientId}` | Consulta estado de bloqueo y contador de intentos | `200 OK` (`bloqueado: boolean`, `intentos: int`) |

---

## 💳 Datos de Tarjeta de Prueba (Webpay Plus Integración)

* **Tarjeta Crédito (Aprobada):** `6623` (o `4000 0000 0000 6623`), Fecha: `12/28`, CVV: `123`.
* **Tarjeta Rechazada (Saldo Insuficiente):** `6624` (o `4000 0000 0000 6624`).
* **Tarjeta Inválida:** `6625` (o `4000 0000 0000 6625`).

---

## 🐳 Despliegue con Docker

El servicio incluye un `Dockerfile` multi-stage:
```bash
# Construir imagen
docker build -t pedidos360-pago-api .

# Ejecutar contenedor
docker run -d --name pedidos360-pago-api -p 8085:8085 pedidos360-pago-api
```
