package com.kafka.pago.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PeticionPagoDTO {
    private String clientId;
    private BigDecimal amount;
    private String transactionId;
    private String returnUrl;
}
