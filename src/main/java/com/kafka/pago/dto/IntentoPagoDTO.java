package com.kafka.pago.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntentoPagoDTO {
    private String idCliente;
    private BigDecimal amount;
    private String transactionId;
    private LocalDateTime timestamp;

    public IntentoPagoDTO(String idCliente, BigDecimal amount, String transactionId) {
        this.idCliente = idCliente;
        this.amount = amount;
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
    }
}
