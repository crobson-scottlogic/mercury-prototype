package com.robsonc.solace.data.jpa.service;

import java.util.concurrent.Future;

public interface MessageBusWrapper {
	Future<Object> writeMessageToQueue(String queueName, String payload);
}
