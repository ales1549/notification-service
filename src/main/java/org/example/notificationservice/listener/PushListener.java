package org.example.notificationservice.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.notificationservice.model.Push;
import org.example.notificationservice.repository.PushRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PushListener {

    private final PushRepository pushRepository;

    @KafkaListener(topics = "push-events", groupId = "notification-group")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                       String message
    ) {

        boolean exists =
                pushRepository.existsByKeyAndValue(key, message);

        if (exists) {
            log.info("Дубликат пропущен. Key: {}", key);
            return;
        }

        Push pushInbox = Push.builder()
                .topic("push-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        pushRepository.save(pushInbox);

        log.info(
                "Получено сообщение из Kafka. Key: {}, Payload: {}, topic: {}", key, message, pushInbox.getTopic()
        );
    }
}
