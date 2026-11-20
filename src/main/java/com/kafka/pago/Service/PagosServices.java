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

@Slf4j
@Service
@RequiredArgsConstructor
public class PagosServices {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EstadoPagoClienteRepositorio estadoRepositorio;

    private static final String TOPIC_NOMBRE = "payment-attempts";

    @Value("${transbank.commerce-code:597055555532}")
    private String commerceCode;

    @Value("${transbank.api-key:579B532A7440BB06BE4A3A796987C214}")
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
                log.info(">>> El bloqueo por 30 minutos expiró para el cliente {}. Desbloqueado automáticamente.", idCliente);
                return false;
            }
        }
        return true;
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
        log.info(">>> [WEBPAY PLUS] Creando transacción en Transbank: buyOrder={}, sessionId={}, amount=${}",
                buyOrder, sessionId, amount);

        WebpayPlus.Transaction tx = WebpayPlus.Transaction.buildForIntegration(
                commerceCode != null ? commerceCode : IntegrationCommerceCodes.WEBPAY_PLUS,
                apiKey != null ? apiKey : IntegrationApiKeys.WEBPAY
        );

        var response = tx.create(buyOrder, sessionId, amount, returnUrl);
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
