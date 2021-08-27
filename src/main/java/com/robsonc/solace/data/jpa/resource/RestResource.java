package com.robsonc.solace.data.jpa.resource;

import java.util.concurrent.atomic.AtomicLong;

import com.robsonc.solace.data.jpa.domain.Message;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestResource {
	private final AtomicLong counter = new AtomicLong();

	@GetMapping("/greeting")
	public Message greeting() {
		return new Message(counter.getAndIncrement(), "Hello, world");
	}
}
