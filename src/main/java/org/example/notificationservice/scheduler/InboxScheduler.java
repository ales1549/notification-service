package org.example.notificationservice.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.notificationservice.config.InboxProperties;
import org.example.notificationservice.model.Email;
import org.example.notificationservice.model.Push;
import org.example.notificationservice.model.Sms;
import org.example.notificationservice.model.Telegram;
import org.example.notificationservice.repository.EmailRepository;
import org.example.notificationservice.repository.PushRepository;
import org.example.notificationservice.repository.SmsRepository;
import org.example.notificationservice.repository.TelegramRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class InboxScheduler {

    private final EmailRepository emailRepository;
    private final PushRepository pushRepository;
    private final SmsRepository smsRepository;
    private final TelegramRepository telegramRepository;
    private final InboxProperties inboxProperties;

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void processEmail(){

        List<Email> messages = emailRepository.findByProcessedFalseOrderByCreatedAtAsc(
                PageRequest.of(0,inboxProperties.getBatchSize())
        );
        for(Email email : messages){
            try{
                log.info(
                        "Обработно событие: Key: {}, Payload: {}, topic: {}",
                        email.getKey(),
                        email.getValue(),
                        email.getTopic()
                );
                email.setProcessed(true);
                emailRepository.save(email);
            } catch(Exception e){
                log.error("Ошибка обработки key={}: {}", email.getKey(), e.getMessage());
                email.setAttempt(email.getAttempt()+1);
                emailRepository.save(email);
            }
        }
    }

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void processPush(){

        List<Push> messages = pushRepository.findByProcessedFalseOrderByCreatedAtAsc(
                PageRequest.of(0,inboxProperties.getBatchSize())
        );
        for(Push push : messages){
            try{
                log.info(
                        "Обработно событие: Key: {}, Payload: {}, topic: {}",
                        push.getKey(),
                        push.getValue(),
                        push.getTopic()
                );
                push.setProcessed(true);
                pushRepository.save(push);
            } catch(Exception e){
                log.error("Ошибка обработки key={}: {}", push.getKey(), e.getMessage());
                push.setAttempt(push.getAttempt()+1);
                pushRepository.save(push);
            }
        }
    }
    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void processSms(){

        List<Sms> messages = smsRepository.findByProcessedFalseOrderByCreatedAtAsc(
                PageRequest.of(0,inboxProperties.getBatchSize())
        );
        for(Sms sms : messages){
            try{
                log.info(
                        "Обработно событие: Key: {}, Payload: {}, topic: {}",
                        sms.getKey(),
                        sms.getValue(),
                        sms.getTopic()
                );
                sms.setProcessed(true);
                smsRepository.save(sms);
            } catch(Exception e){
                log.error("Ошибка обработки key={}: {}", sms.getKey(), e.getMessage());
                sms.setAttempt(sms.getAttempt()+1);
                smsRepository.save(sms);
            }
        }
    }

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void processTelegram(){

        List<Telegram> messages = telegramRepository.findByProcessedFalseOrderByCreatedAtAsc(
                PageRequest.of(0,inboxProperties.getBatchSize())
        );
        for(Telegram telegram : messages){
            try{
                log.info(
                        "Обработно событие: Key: {}, Payload: {}, topic: {}",
                        telegram.getKey(),
                        telegram.getValue(),
                        telegram.getTopic()
                );
                telegram.setProcessed(true);
                telegramRepository.save(telegram);
            } catch(Exception e){
                log.error("Ошибка обработки key={}: {}", telegram.getKey(), e.getMessage());
                telegram.setAttempt(telegram.getAttempt()+1);
                telegramRepository.save(telegram);
            }
        }
    }
}
