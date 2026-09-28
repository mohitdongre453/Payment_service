package com.amazon.payment.service;

import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.amazon.payment.entity.PaymentEntity;
import com.amazon.payment.event.OrderCreatedEvent;
import com.amazon.payment.event.PaymentFailedEvent;
import com.amazon.payment.event.PaymentSuccessEvent;
import com.amazon.payment.repository.PaymentRepository;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentService(
            PaymentRepository paymentRepository,
            KafkaTemplate<String, Object> kafkaTemplate) {

        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void processPayment(OrderCreatedEvent event) {

        System.out.println(
                "Processing payment for orderId: "
                + event.getOrderId()
        );

        // =====================================================
        // 1. IDEMPOTENCY CHECK
        // =====================================================

        if (paymentRepository.findByEventId(event.getEventId()).isPresent()) {

            System.out.println(
                    "Duplicate event detected. "
                    + "Skipping payment processing."
            );

            System.out.println(
                    "Event ID: " + event.getEventId()
            );

            return;
        }

        // =====================================================
        // 2. CREATE PAYMENT ID
        // =====================================================

        String paymentId = "PAY-" + UUID.randomUUID();

        // =====================================================
        // 3. PAYMENT SIMULATION
        // =====================================================
        //
        // amount <= 50000  -> SUCCESS
        // amount > 50000   -> FAILED
        //

        boolean paymentSuccess =
                event.getAmount() <= 50000;

        // =====================================================
        // 4. CREATE PAYMENT ENTITY
        // =====================================================

        PaymentEntity payment = new PaymentEntity();

        payment.setEventId(event.getEventId());
        payment.setOrderId(event.getOrderId());
        payment.setCustomerId(event.getCustomerId());
        payment.setAmount(event.getAmount());
        payment.setPaymentId(paymentId);
        payment.setPaymentMethod("UPI");

        // =====================================================
        // 5. PAYMENT SUCCESS
        // =====================================================

        if (paymentSuccess) {

            payment.setPaymentStatus("SUCCESS");
            payment.setReason(null);

            PaymentEntity savedPayment =
                    paymentRepository.save(payment);

            System.out.println(
                    "Payment SUCCESS for orderId: "
                    + savedPayment.getOrderId()
            );

            // Create success event

            PaymentSuccessEvent successEvent =
                    new PaymentSuccessEvent();

            successEvent.setEventId("PAY-" + UUID.randomUUID());
            successEvent.setEventType("PAYMENT_SUCCESS");
            successEvent.setOrderId(event.getOrderId());
            successEvent.setCustomerId(event.getCustomerId());
            successEvent.setAmount(event.getAmount());
            successEvent.setPaymentId(paymentId);
            successEvent.setPaymentMethod("UPI");
            successEvent.setPaymentStatus("SUCCESS");

            // Publish using orderId as Kafka key

            kafkaTemplate.send(
                    "payment-success",
                    String.valueOf(event.getOrderId()),
                    successEvent
            );

            System.out.println(
                    "PAYMENT_SUCCESS event published "
                    + "for orderId: "
                    + event.getOrderId()
            );
        }

        // =====================================================
        // 6. PAYMENT FAILED
        // =====================================================

        else {

            payment.setPaymentStatus("FAILED");
            payment.setReason("INSUFFICIENT_FUNDS");

            PaymentEntity savedPayment =
                    paymentRepository.save(payment);

            System.out.println(
                    "Payment FAILED for orderId: "
                    + savedPayment.getOrderId()
            );

            // Create failed event

            PaymentFailedEvent failedEvent =
                    new PaymentFailedEvent();

            failedEvent.setEventId("PAY-" + UUID.randomUUID());
            failedEvent.setEventType("PAYMENT_FAILED");
            failedEvent.setOrderId(event.getOrderId());
            failedEvent.setCustomerId(event.getCustomerId());
            failedEvent.setAmount(event.getAmount());
            failedEvent.setPaymentId(paymentId);
            failedEvent.setPaymentStatus("FAILED");
            failedEvent.setReason("INSUFFICIENT_FUNDS");

            // Publish using orderId as Kafka key

            kafkaTemplate.send(
                    "payment-failed",
                    String.valueOf(event.getOrderId()),
                    failedEvent
            );

            System.out.println(
                    "PAYMENT_FAILED event published "
                    + "for orderId: "
                    + event.getOrderId()
            );
        }
    }
}