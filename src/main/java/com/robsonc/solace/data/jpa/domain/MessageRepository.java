package com.robsonc.solace.data.jpa.domain;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRepository extends JpaRepository<Message, Long> {
	Optional<Message> findById(Long id);
	List<Message> findByContent(String content);
}
