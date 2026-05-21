package org.example.notificationservice.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.notificationservice.model.Telegram;
import org.example.notificationservice.repository.TelegramRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelegramListener {

    private final TelegramRepository telegramRepository;

    @KafkaListener(topics = "telegram-events", groupId = "notification-group")
    public void listen(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                       String message
    ) {

        boolean exists =
                telegramRepository.existsByKeyAndValue(key, message);

        if (exists) {
            log.info("Дубликат пропущен. Key: {}", key);
            return;
        }

        Telegram telegramInbox = Telegram.builder()
                .topic("telegram-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        telegramRepository.save(telegramInbox);

        log.info(
                "Получено сообщение из Kafka. Key: {}, Payload: {}, topic: {}", key, message, telegramInbox.getTopic()
        );
    }
}
