package com.robsonc.solace.data.jpa.service;

import java.util.List;

import com.robsonc.solace.data.jpa.domain.MessageRepository;
import com.robsonc.solace.data.jpa.domain.QueueMessage;
import com.robsonc.solace.data.jpa.domain.SearchParameters;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SearchService {

	@Autowired
	private MessageRepository repository;

	public List<QueueMessage> search(SearchParameters parameters) {
		// for now, just do exact string matching on content
		return repository.findByContent(parameters.getSearchText());
	}
}
