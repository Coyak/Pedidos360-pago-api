package com.kafka.pago.Controller;

import cl.transbank.common.IntegrationApiKeys;
import cl.transbank.common.IntegrationCommerceCodes;
import cl.transbank.webpay.webpayplus.WebpayPlus;
import com.kafka.pago.Model.EstadoPagoCliente;
import com.kafka.pago.Repository.EstadoPagoClienteRepositorio;
import com.kafka.pago.Service.PagosServices;
import com.kafka.pago.dto.PeticionPagoDTO;
import com.kafka.pago.dto.TransaccionRespuestaDTO;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/pagos")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class PagoController {

    private final PagosServices pagosServices;
    private final EstadoPagoClienteRepositorio estadoRepositorio;

    @Value("${transbank.return-url:https://pedidos360.cloud/callback}")
    private String defaultReturnUrl;

    @Value("${transbank.commerce-code:597055555532}")
    private String commerceCode;

    @Value("${transbank.api-key:579B532A7440BB0C9079DED94D31EA1615BACEB56610332264630D42D0A36B1C}")
    private String apiKey;

    /**
     * Endpoint de salud y diagnóstico del microservicio de Pagos
     */
    @GetMapping("/status")
    public ResponseEntity<?> getStatus() {
        return ResponseEntity.ok(Map.of(
                "service", "Pedidos360-pago-api",
                "status", "UP",
                "module", "EV3 Pagos & Antifraude + Webpay Plus",
                "commerceCode", commerceCode,
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Consulta el estado de seguridad y bloqueo de un cliente
     */
    @GetMapping("/estado-cliente/{clientId}")
    public ResponseEntity<?> getEstadoCliente(@PathVariable String clientId) {
        EstadoPagoCliente estado = estadoRepositorio.findById(clientId).orElse(null);
        if (estado == null) {
            return ResponseEntity.ok(Map.of(
                    "idCliente", clientId,
                    "bloqueado", false,
                    "cantidadIntentos", 0
            ));
        }
        return ResponseEntity.ok(estado);
    }

    /**
     * 1. Solicitud de Pago y Validación de Bloqueo Antifraude (/api/pagos/iniciar)
     * Requisito y Pauta de Clase exacta: >3 intentos en <10 segundos -> HTTP 429 Too Many Requests
     */
    @PostMapping("/iniciar")
    public ResponseEntity<?> iniciarPago(@RequestBody PeticionPagoDTO peticion, HttpServletRequest httpRequest) {
        log.info(">>> [POST /api/pagos/iniciar] Petición recibida para cliente: {}, monto: ${}",
                peticion.getClientId(), peticion.getAmount());

        if (peticion.getClientId() == null || peticion.getClientId().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "El clientId es obligatorio"));
        }

        if (peticion.getTransactionId() == null || peticion.getTransactionId().trim().isEmpty()) {
            peticion.setTransactionId("TX-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 4));
        }

        // 1.1 Publicar intento de pago hacia Kafka (Velocity Check Topic)
        pagosServices.producirIntentoPago(peticion.getClientId(), peticion.getAmount(), peticion.getTransactionId());

        // 1.2 Validación síncrona + ventana deslizante (>3 intentos en <=10s)
        if (pagosServices.verificarSiUsuarioEstaBloqueado(peticion.getClientId())
                || pagosServices.evaluarVentanaVelocidadYBloquear(peticion.getClientId())) {
            log.warn(">>> [BLOQUEO 429] Intento de pago rechazado para cliente penalizado: {}", peticion.getClientId());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                    "status", "BLOQUEADO_POR_FRAUDE",
                    "mensaje", "Usuario bloqueado preventivamente por sospecha de fraude (más de 3 intentos en menos de 10 segundos). Por favor intente nuevamente en 30 minutos.",
                    "bloqueado", true,
                    "tiempoCastigoMinutos", 30
            ));
        }

        // 1.3 Integración con Webpay Plus REST
        try {
            String returnUrl = resolverReturnUrl(peticion.getReturnUrl(), httpRequest);

            TransaccionRespuestaDTO transaccionWebpay = pagosServices.iniciarTransaccionWebpay(
                    peticion.getTransactionId(),
                    peticion.getClientId(),
                    peticion.getAmount() != null ? peticion.getAmount().doubleValue() : 10000.0,
                    returnUrl
            );

            return ResponseEntity.ok(Map.of(
                    "status", "INICIADO",
                    "url", transaccionWebpay.getUrlWebpay(),
                    "token", transaccionWebpay.getToken(),
                    "transactionId", peticion.getTransactionId()
            ));

        } catch (Exception e) {
            log.error(">>> [ERROR WEBPAY] No se pudo crear transacción en Transbank: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Error al conectar con la pasarela de pagos Webpay: " + e.getMessage()));
        }
    }

    private String resolverReturnUrl(String requestedReturnUrl, HttpServletRequest httpRequest) {
        if (requestedReturnUrl != null && (requestedReturnUrl.startsWith("http://") || requestedReturnUrl.startsWith("https://"))) {
            return requestedReturnUrl;
        }
        String origin = httpRequest != null ? httpRequest.getHeader("Origin") : null;
        if (origin != null && (origin.startsWith("http://") || origin.startsWith("https://"))) {
            return origin.replaceAll("/$", "") + "/callback";
        }
        if (defaultReturnUrl != null && (defaultReturnUrl.startsWith("http://") || defaultReturnUrl.startsWith("https://"))) {
            return defaultReturnUrl;
        }
        String host = (httpRequest != null && httpRequest.getServerName() != null && !httpRequest.getServerName().isBlank())
                ? httpRequest.getServerName()
                : "pedidos360.cloud";
        String scheme = (httpRequest != null && httpRequest.getScheme() != null && !httpRequest.getScheme().isBlank())
                ? httpRequest.getScheme()
                : "https";
        return scheme + "://" + host + "/callback";
    }

    /**
     * 2. Confirmación definitiva de la Transacción en Webpay Plus (/api/pagos/commit)
     * Soporta token_ws por Query Param o por Body JSON
     */
    @PostMapping("/commit")
    public ResponseEntity<?> confirmarPagoWebpay(
            @RequestParam(value = "token_ws", required = false) String tokenParam,
            @RequestBody(required = false) Map<String, String> body) {

        String tokenWs = tokenParam;
        if ((tokenWs == null || tokenWs.isEmpty()) && body != null) {
            tokenWs = body.get("token_ws");
            if (tokenWs == null) {
                tokenWs = body.get("token");
            }
        }

        if (tokenWs == null || tokenWs.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "token_ws es requerido para el commit"));
        }

        try {
            String effectiveCommerceCode = (commerceCode != null && !commerceCode.isBlank())
                    ? commerceCode.trim()
                    : IntegrationCommerceCodes.WEBPAY_PLUS;
            String effectiveApiKey = (apiKey != null && apiKey.trim().length() >= 60 && !IntegrationCommerceCodes.WEBPAY_PLUS.equals(effectiveCommerceCode))
                    ? apiKey.trim()
                    : IntegrationApiKeys.WEBPAY;

            WebpayPlus.Transaction tx = WebpayPlus.Transaction.buildForIntegration(
                    effectiveCommerceCode,
                    effectiveApiKey
            );

            var response = tx.commit(tokenWs);
            log.info(">>> [WEBPAY COMMIT] Estado: {}, Orden: {}, Monto: ${}",
                    response.getStatus(), response.getBuyOrder(), response.getAmount());

            boolean autorizado = "AUTHORIZED".equals(response.getStatus());
            pagosServices.gestionarResultadoCallbackPago(
                    response.getSessionId(),
                    response.getBuyOrder(),
                    response.getAmount(),
                    autorizado
            );

            if (autorizado) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "status", response.getStatus(),
                        "buyOrder", response.getBuyOrder(),
                        "amount", response.getAmount(),
                        "authorizationCode", response.getAuthorizationCode(),
                        "paymentTypeCode", response.getPaymentTypeCode(),
                        "responseCode", response.getResponseCode(),
                        "mensaje", "¡Pago Aprobado exitosamente con Webpay Plus!"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "status", response.getStatus(),
                        "buyOrder", response.getBuyOrder(),
                        "mensaje", "El pago fue rechazado por Transbank."
                ));
            }

        } catch (Exception e) {
            log.error(">>> [ERROR COMMIT] Error al confirmar con Transbank: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Error al confirmar la transacción: " + e.getMessage()));
        }
    }
}
