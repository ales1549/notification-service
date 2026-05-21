package org.example.notificationservice.repository;

import org.example.notificationservice.model.Sms;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface SmsRepository extends JpaRepository<Sms, UUID> {

    boolean existsByKeyAndValue(String key, String value);

    List<Sms> findByProcessedFalseOrderByCreatedAtAsc(Pageable pageable);

}
