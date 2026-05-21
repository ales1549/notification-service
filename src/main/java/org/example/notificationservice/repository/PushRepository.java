package org.example.notificationservice.repository;

import org.example.notificationservice.model.Push;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface PushRepository extends JpaRepository<Push, UUID> {

    boolean existsByKeyAndValue(String key, String value);

    List<Push> findByProcessedFalseOrderByCreatedAtAsc(Pageable pageable);

}
