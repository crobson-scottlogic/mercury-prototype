package com.robsonc.solace.data.jpa.resource;

import java.sql.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.robsonc.solace.data.jpa.domain.DestinationType;
import com.robsonc.solace.data.jpa.domain.MessageInDto;
import com.robsonc.solace.data.jpa.domain.MessageRepository;
import com.robsonc.solace.data.jpa.domain.QueueMessage;
import com.robsonc.solace.data.jpa.domain.SearchParameters;
import com.robsonc.solace.data.jpa.service.MessageBusWrapper;
import com.robsonc.solace.data.jpa.service.SearchService;
import com.robsonc.solace.data.jpa.service.SolaceWrapper.MessageWithId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestResource {
	@Autowired
	private MessageBusWrapper messageBusWrapper;

	@Autowired
	private MessageRepository repository;

	@Autowired
	private SearchService searchService;

	@PostMapping(value = "/publish", path = "/publish", consumes = "APPLICATION/JSON", produces = "APPLICATION/JSON")
	public ResponseEntity<String> publishToMessageBus(@RequestBody MessageInDto messageInDto) {
		//System.out.println(messageInDto);
		List<QueueMessage> messages = repository.findAll();
		System.out.println("Requested messages. Got list of size: " + messages.size());
		for (var message : messages) {
			System.out.println(message.toString());
		}

		var uuid = UUID.randomUUID().toString();
		var message = new QueueMessage(messageInDto.getPayload(), messageInDto.getMessageVpn(), messageInDto.getDestination(), DestinationType.valueOf(messageInDto.getDestinationType().toUpperCase()), uuid);
		repository.save(message);

		var future = messageBusWrapper.writeMessageToQueue(messageInDto.getDestination(), messageInDto.getPayload(), uuid, messageInDto.getMessageVpn());
		try {
			Object key = future.get(10, TimeUnit.SECONDS);
			String responseBody = "Received: " + key;
			return ResponseEntity.ok(responseBody);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping(value = "/getall", produces = "APPLICATION/JSON")
	public List<MessageWithId> getLatestMessages(@RequestParam(name = "queue") String queueName, @RequestParam(name = "vpn", required = false) String vpn) {
		System.out.println("============================");
		System.out.println("queueName: " + queueName);
		System.out.println("============================");
		return messageBusWrapper.getLatestUnreadMessages(queueName, vpn);
	}

	@GetMapping(value = "/get", produces = "APPLICATION/JSON")
	public MessageWithId getLatestMessage(@RequestParam(name = "queue") String queueName, @RequestParam(name = "vpn", required = false) String vpn) {
		return messageBusWrapper.getLatestUnreadMessage(queueName, vpn);
	}
	
	@GetMapping(value = "/next", produces = "APPLICATION/JSON")
	public MessageWithId getEarliestUnackedMessage(@RequestParam(name = "queue") String queueName, @RequestParam(name = "vpn", required = false) String vpn) {
		return messageBusWrapper.getEarliestUnreadMessage(queueName, vpn);
	}
	
	@GetMapping(value = "/ack")
	public void ackMessage(@RequestParam(name = "queue") String queueName, @RequestParam(name = "msg", required = false) String messageId, @RequestParam(name = "vpn", required = false) String vpn) {
		System.out.println("Message ID is: " + messageId);
		messageBusWrapper.ackMessage(queueName, messageId, vpn);
	}

	@GetMapping(value = "/replay", produces = "APPLICATION/JSON")
	public List<MessageWithId> replay(@RequestParam(name = "queue") String queueName, @RequestParam(name = "replayTime", required = false) Long replayTime, @RequestParam(name = "vpn", required = false) String vpn) {
		System.out.println("Replaying queue " + queueName + " from " + new java.util.Date(replayTime));
		try {
			return messageBusWrapper.replay(queueName, replayTime, vpn);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@GetMapping(value = "/search", produces = "APPLICATION/JSON")
	public List<MessageWithId> search(
			@RequestParam(name = "messageVpn", required = false) String messageVpn,
			@RequestParam(name = "queueName", required = false) String queueName,
			@RequestParam(name = "destination", required = false) String destination,
			@RequestParam(name = "earliestPublished", required = false) Date earliestPublished,
			@RequestParam(name = "latestPublished", required = false) Date latestPublished,
			@RequestParam(name = "searchText", required = false) String searchText) {
		SearchParameters parameters = SearchParameters.builder()
			.messageVpn(messageVpn)
			.queueName(queueName)
			.destination(destination)
			.earliestPublished(earliestPublished)
			.latestPublished(latestPublished)
			.searchText(searchText)
			.build();
		System.out.println(parameters.toString());
		var matchingMessages = searchService.search(parameters);
		// actually, I think search will need a lot more information than this - e.g. publisher name, published date, etc.
		return matchingMessages.stream()
			.map(msg -> new MessageWithId(msg.getApplicationId(), msg.getContent()))
			.collect(Collectors.toList());
	}
}
