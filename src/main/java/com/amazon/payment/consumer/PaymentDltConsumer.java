package com.amazon.payment.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentDltConsumer {

    @KafkaListener(
            topics = "order-created.DLT",
            groupId = "payment-dlt-group"
    )
    public void consumeDlt(String message) {

        System.out.println();
        System.out.println("======================================");
        System.out.println("       PAYMENT DLT CONSUMER");
        System.out.println("======================================");

        System.out.println(
                "Failed message received from DLT:"
        );

        System.out.println(message);

        System.out.println(
                "======================================"
        );
    }
}