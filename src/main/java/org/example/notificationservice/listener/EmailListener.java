package org.example.notificationservice.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.notificationservice.model.Email;
import org.example.notificationservice.repository.EmailRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailListener {

    private final EmailRepository emailRepository;

    @KafkaListener(topics = "email-events", groupId = "notification-group")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                       String message
    ) {

        boolean exists =
                emailRepository.existsByKeyAndValue(key, message);

        if (exists) {
            log.info("Дубликат пропущен. Key: {}", key);
            return;
        }

        Email emailInbox = Email.builder()
                .topic("email-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        emailRepository.save(emailInbox);

        log.info(
                "Получено сообщение из Kafka. Key: {}, Payload: {}, topic: {}", key, message, emailInbox.getTopic()
        );
    }
}
