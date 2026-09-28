package com.amazon.payment.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.amazon.payment.event.OrderCreatedEvent;
import com.amazon.payment.service.PaymentService;

@Component
public class PaymentConsumer {

    private final PaymentService paymentService;

    public PaymentConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(
            topics = "order-created",
            groupId = "payment-service-group"
    )
    public void consumeOrderCreated(OrderCreatedEvent event) {

        System.out.println(
                "Received ORDER_CREATED event for orderId: "
                + event.getOrderId()
        );

        paymentService.processPayment(event);
    }
}
