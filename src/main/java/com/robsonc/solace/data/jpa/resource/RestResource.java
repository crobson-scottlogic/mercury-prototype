package com.robsonc.solace.data.jpa.resource;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.robsonc.solace.data.jpa.domain.Message;
import com.robsonc.solace.data.jpa.domain.MessageInDto;
import com.robsonc.solace.data.jpa.domain.MessageRepository;
import com.robsonc.solace.data.jpa.service.MessageBusWrapper;
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
	private final AtomicLong counter = new AtomicLong();

	@Autowired
	private MessageBusWrapper messageBusWrapper;

	@Autowired
	private MessageRepository repository;

	@GetMapping("/greeting")
	public Message greeting() {
		return new Message(counter.getAndIncrement(), "Hello, world");
	}

	@PostMapping(value = "/publish", path = "/publish", consumes = "APPLICATION/JSON", produces = "APPLICATION/JSON")
	public ResponseEntity<String> publishToMessageBus(@RequestBody MessageInDto messageInDto) {
		System.out.println("============================");
		System.out.println(messageInDto);
		System.out.println("============================");
		List<Message> messages = repository.findAll();
		System.out.println("Requested messages. Got list of size: " + messages.size());
		Message message = new Message(messageInDto.getPayload());
		//dcfgdhtgtf;
		repository.save(message);
		var future = messageBusWrapper.writeMessageToQueue(messageInDto.getDestination(), messageInDto.getPayload());
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
	public List<MessageWithId> getLatestMessages(@RequestParam(name = "queue") String queueName) {
		System.out.println("============================");
		System.out.println("queueName: " + queueName);
		System.out.println("============================");
		return messageBusWrapper.getLatestUnreadMessages(queueName);
	}

	@GetMapping(value = "/get", produces = "APPLICATION/JSON")
	public MessageWithId getLatestMessage(@RequestParam(name = "queue") String queueName) {
		return messageBusWrapper.getLatestUnreadMessage(queueName);
	}
	
	@GetMapping(value = "/ack")
	public void ackMessage(@RequestParam(name = "queue") String queueName, @RequestParam(name = "msg", required = false) String messageId) {
		System.out.println("Message ID is: " + messageId);
		messageBusWrapper.ackMessage(queueName, messageId);
	}

	@GetMapping(value = "/replay")
	public void replay() {
		
	}
}
