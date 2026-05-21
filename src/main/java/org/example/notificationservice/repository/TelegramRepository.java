package org.example.notificationservice.repository;

import org.example.notificationservice.model.Telegram;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TelegramRepository extends JpaRepository<Telegram, UUID> {

    boolean existsByKeyAndValue(String key, String value);

    List<Telegram> findByProcessedFalseOrderByCreatedAtAsc(Pageable pageable);

}
