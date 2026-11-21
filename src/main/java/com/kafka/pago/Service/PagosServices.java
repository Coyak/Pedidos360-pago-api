package com.kafka.pago.Service;

import cl.transbank.common.IntegrationApiKeys;
import cl.transbank.common.IntegrationCommerceCodes;
import cl.transbank.webpay.webpayplus.WebpayPlus;
import com.kafka.pago.Model.EstadoPagoCliente;
import com.kafka.pago.Repository.EstadoPagoClienteRepositorio;
import com.kafka.pago.dto.IntentoPagoDTO;
import com.kafka.pago.dto.TransaccionRespuestaDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class PagosServices {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EstadoPagoClienteRepositorio estadoRepositorio;

    private static final String TOPIC_NOMBRE = "payment-attempts";
    private static final String TOPIC_ALERTAS_FRAUDE = "pedidos.alertas.fraude";

    // Ventana deslizante en memoria para detección inmediata de ráfagas concurrentes (>3 intentos en <=10s)
    private final Map<String, List<Long>> ventanaIntentosCliente = new ConcurrentHashMap<>();

    @Value("${transbank.commerce-code:597055555532}")
    private String commerceCode;

    @Value("${transbank.api-key:579B532A7440BB0C9079DED94D31EA1615BACEB56610332264630D42D0A36B1C}")
    private String apiKey;

    /**
     * Verifica de forma síncrona en base de datos si el cliente está penalizado por la regla antifraude.
     * Si ya pasaron los 30 minutos de castigo, se desbloquea automáticamente.
     */
    public boolean verificarSiUsuarioEstaBloqueado(String idCliente) {
        EstadoPagoCliente estado = estadoRepositorio.findById(idCliente).orElse(null);
        if (estado == null || !estado.isBloqueado()) {
            return false;
        }

        if (estado.getBloqueadoHasta() != null) {
            LocalDateTime ahora = LocalDateTime.now();
            if (ahora.isAfter(estado.getBloqueadoHasta())) {
                estado.setBloqueado(false);
                estado.setCantidadIntentos(0);
                estado.setBloqueadoHasta(null);
                estado.setActualizadoEn(ahora);
                estadoRepositorio.save(estado);
                ventanaIntentosCliente.remove(idCliente);
                log.info(">>> El bloqueo por 30 minutos expiró para el cliente {}. Desbloqueado automáticamente.", idCliente);
                return false;
            }
        }
        return true;
    }

    /**
     * Evalúa de forma atómica si el cliente supera 3 intentos en una ventana de 10 segundos.
     * Si supera el umbral (>3 intentos en <=10s), persiste el bloqueo en MySQL y emite alerta a Kafka.
     */
    public synchronized boolean evaluarVentanaVelocidadYBloquear(String idCliente) {
        long ahoraMs = System.currentTimeMillis();
        List<Long> marcas = ventanaIntentosCliente.computeIfAbsent(idCliente, k -> new ArrayList<>());
        marcas.removeIf(ts -> (ahoraMs - ts) > 10_000L);
        marcas.add(ahoraMs);

        if (marcas.size() > 3) {
            LocalDateTime ahora = LocalDateTime.now();
            EstadoPagoCliente estado = estadoRepositorio.findById(idCliente).orElseGet(() -> {
                EstadoPagoCliente nuevo = new EstadoPagoCliente();
                nuevo.setIdCliente(idCliente);
                return nuevo;
            });
            estado.setCantidadIntentos(marcas.size());
            estado.setBloqueado(true);
            estado.setBloqueadoHasta(ahora.plusMinutes(30));
            estado.setActualizadoEn(ahora);
            estadoRepositorio.save(estado);

            publicarAlertaFraudeKafka(idCliente, marcas.size());
            return true;
        }
        return false;
    }

    public void publicarAlertaFraudeKafka(String idCliente, int intentos) {
        try {
            Map<String, Object> alerta = Map.of(
                    "clientId", idCliente,
                    "razon", "Ráfaga de " + intentos + " intentos en < 10 segundos (Bloqueo 30 min HTTP 429)",
                    "ip", "190.161.88.94",
                    "timestamp", System.currentTimeMillis()
            );
            kafkaTemplate.send(TOPIC_ALERTAS_FRAUDE, idCliente, alerta);
            log.info(">>> [KAFKA FRAUD ALERT] Alerta publicada en '{}' para {}", TOPIC_ALERTAS_FRAUDE, idCliente);
        } catch (Exception e) {
            log.warn("No se pudo publicar alerta de fraude en Kafka: {}", e.getMessage());
        }
    }

    /**
     * Publica un intento de pago de forma asíncrona hacia el bus de Kafka para análisis de velocidad.
     */
    public void producirIntentoPago(String idCliente, BigDecimal amount, String transactionId) {
        IntentoPagoDTO intento = new IntentoPagoDTO(idCliente, amount, transactionId);
        try {
            kafkaTemplate.send(TOPIC_NOMBRE, idCliente, intento);
            log.info(">>> [KAFKA PRODUCER] Intento de pago enviado a '{}' para cliente: {}", TOPIC_NOMBRE, idCliente);
        } catch (Exception e) {
            log.warn(">>> [KAFKA WARN] No se pudo enviar intento a Kafka ({}). Continuando flujo.", e.getMessage());
        }
    }

    /**
     * Crea una transacción real en el ambiente de integración oficial de Transbank Webpay Plus.
     */
    public TransaccionRespuestaDTO iniciarTransaccionWebpay(String buyOrder, String sessionId, double amount, String returnUrl) throws Exception {
        String cleanBuyOrder = (buyOrder != null && buyOrder.length() > 26) ? buyOrder.substring(0, 26) : buyOrder;
        String cleanSessionId = (sessionId != null && sessionId.length() > 61) ? sessionId.substring(0, 61) : sessionId;
        double cleanAmount = amount > 0 ? Math.round(amount) : 10000.0;

        String effectiveCommerceCode = (commerceCode != null && !commerceCode.isBlank())
                ? commerceCode.trim()
                : IntegrationCommerceCodes.WEBPAY_PLUS;
        String effectiveApiKey = (apiKey != null && apiKey.trim().length() >= 60 && !IntegrationCommerceCodes.WEBPAY_PLUS.equals(effectiveCommerceCode))
                ? apiKey.trim()
                : IntegrationApiKeys.WEBPAY;

        log.info(">>> [WEBPAY PLUS] Creando transacción en Transbank: commerceCode={}, buyOrder={}, sessionId={}, amount=${}, returnUrl={}",
                effectiveCommerceCode, cleanBuyOrder, cleanSessionId, cleanAmount, returnUrl);

        WebpayPlus.Transaction tx = WebpayPlus.Transaction.buildForIntegration(
                effectiveCommerceCode,
                effectiveApiKey
        );

        var response = tx.create(cleanBuyOrder, cleanSessionId, cleanAmount, returnUrl);
        log.info(">>> [WEBPAY OK] Token generado: {}, Url: {}", response.getToken(), response.getUrl());
        return new TransaccionRespuestaDTO(response.getUrl(), response.getToken());
    }

    /**
     * Callback de confirmación tras el retorno de Webpay.
     */
    public void gestionarResultadoCallbackPago(String idCliente, boolean aprobado) {
        log.info(">>> [CALLBACK PAGO] Resultado para cliente {}: aprobado={}", idCliente, aprobado);
    }
}
