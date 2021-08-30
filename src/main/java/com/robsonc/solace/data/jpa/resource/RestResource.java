package com.robsonc.solace.data.jpa.resource;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.robsonc.solace.data.jpa.domain.Message;
import com.robsonc.solace.data.jpa.domain.MessageInDto;
import com.robsonc.solace.data.jpa.service.MessageBusWrapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestResource {
	private final AtomicLong counter = new AtomicLong();

	@Autowired
	private MessageBusWrapper messageBusWrapper;

	@GetMapping("/greeting")
	public Message greeting() {
		return new Message(counter.getAndIncrement(), "Hello, world");
	}

	@PostMapping(value = "/publish", path = "/publish", consumes = "APPLICATION/JSON", produces = "APPLICATION/JSON")
	public ResponseEntity publishToMessageBus(@RequestBody MessageInDto messageInDto) {
		System.out.println("============================");
		System.out.println(messageInDto);
		System.out.println("============================");
		var future = messageBusWrapper.writeMessageToQueue(messageInDto.getQueueName(), messageInDto.getPayload());
		try {
			Object key = future.get(10, TimeUnit.SECONDS);
			String responseBody = "Received: " + key;
			return ResponseEntity.ok(responseBody);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			return ResponseEntity.internalServerError().build();
		}
	}
}
