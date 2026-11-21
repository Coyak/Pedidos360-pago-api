package com.kafka.pago.Component;

import com.kafka.pago.Model.EstadoPagoCliente;
import com.kafka.pago.Repository.EstadoPagoClienteRepositorio;
import com.kafka.pago.Service.PagosServices;
import com.kafka.pago.dto.IntentoPagoDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Consumidor de Kafka para el Velocity Check antifraude (Requisito y Pauta de Clase).
 * Evalúa si una misma cuenta o tarjeta se repite más de 3 veces en una ventana de 10 segundos,
 * aplicando un bloqueo preventivo de 30 minutos.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumidorComponent {

    private static final String TOPIC_NOMBRE = "payment-attempts";

    private final EstadoPagoClienteRepositorio estadoRepositorio;
    private final PagosServices pagosServices;

    @KafkaListener(topics = TOPIC_NOMBRE, groupId = "payment-velocity-group")
    @Transactional
    public void consumirIntentoPago(IntentoPagoDTO evento) {
        if (evento == null || evento.getIdCliente() == null) {
            return;
        }

        log.info(">>> [KAFKA VELOCITY CHECK] Procesando intento de pago para cliente: {}", evento.getIdCliente());
        LocalDateTime ahora = LocalDateTime.now();

        EstadoPagoCliente estado = estadoRepositorio.findById(evento.getIdCliente())
                .orElseGet(() -> {
                    EstadoPagoCliente nuevo = new EstadoPagoCliente();
                    nuevo.setIdCliente(evento.getIdCliente());
                    nuevo.setCantidadIntentos(0);
                    nuevo.setBloqueado(false);
                    return nuevo;
                });

        // 1. Si está bloqueado, revisar si los 30 minutos ya pasaron
        if (estado.isBloqueado() && estado.getBloqueadoHasta() != null) {
            if (ahora.isBefore(estado.getBloqueadoHasta())) {
                log.warn(">>> [ANTIFRAUDE] El cliente {} sigue bloqueado hasta {}", evento.getIdCliente(), estado.getBloqueadoHasta());
                return;
            } else {
                estado.setBloqueado(false);
                estado.setCantidadIntentos(0);
                estado.setBloqueadoHasta(null);
                log.info(">>> [ANTIFRAUDE] Castigo de 30 min expiró para {}. Desbloqueado.", evento.getIdCliente());
            }
        }

        // 2. Evaluar ventana de 10 segundos (> 3 intentos en <= 10s)
        if (estado.getActualizadoEn() != null) {
            long segundosTranscurridos = Duration.between(estado.getActualizadoEn(), ahora).toSeconds();
            if (segundosTranscurridos <= 10) {
                estado.setCantidadIntentos(estado.getCantidadIntentos() + 1);
                if (estado.getCantidadIntentos() > 3) {
                    estado.setBloqueado(true);
                    estado.setBloqueadoHasta(ahora.plusMinutes(30));
                    log.error("🚨 [ALERTA ANTIFRAUDE] Cliente {} BLOQUEADO POR 30 MINUTOS: {} intentos registrados en {} segundos.",
                            evento.getIdCliente(), estado.getCantidadIntentos(), segundosTranscurridos);
                    pagosServices.publicarAlertaFraudeKafka(evento.getIdCliente(), estado.getCantidadIntentos());
                }
            } else {
                estado.setCantidadIntentos(1);
            }
        } else {
            estado.setCantidadIntentos(1);
        }

        estado.setActualizadoEn(ahora);
        estadoRepositorio.save(estado);
        log.info(">>> Estado cliente {}: intentos={}, bloqueado={}", evento.getIdCliente(), estado.getCantidadIntentos(), estado.isBloqueado());
    }
}
