package com.robsonc.solace.data.jpa.domain;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRepository extends JpaRepository<QueueMessage, Long> {
	Optional<QueueMessage> findById(Long id);
	List<QueueMessage> findByContent(String content);
}
