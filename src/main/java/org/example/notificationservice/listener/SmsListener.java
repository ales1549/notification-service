package org.example.notificationservice.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.notificationservice.model.Sms;
import org.example.notificationservice.repository.SmsRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SmsListener {

    private final SmsRepository smsRepository;

    @KafkaListener(topics = "sms-events", groupId = "notification-group")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                       String message
    ) {

        boolean exists =
                smsRepository.existsByKeyAndValue(key, message);

        if (exists) {
            log.info("Дубликат пропущен. Key: {}", key);
            return;
        }

        Sms smsInbox = Sms.builder()
                .topic("sms-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        smsRepository.save(smsInbox);

        log.info(
                "Получено сообщение из Kafka. Key: {}, Payload: {}, topic: {}", key, message, smsInbox.getTopic()
        );
    }
}
