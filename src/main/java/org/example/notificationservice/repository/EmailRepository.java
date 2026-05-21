package org.example.notificationservice.repository;

import org.example.notificationservice.model.Email;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface EmailRepository extends JpaRepository<Email, UUID> {

    boolean existsByKeyAndValue(String key, String value);

    List<Email> findByProcessedFalseOrderByCreatedAtAsc(Pageable pageable);

}
