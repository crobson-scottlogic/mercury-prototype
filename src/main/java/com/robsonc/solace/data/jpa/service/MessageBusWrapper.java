package com.robsonc.solace.data.jpa.service;

import java.util.List;
import java.util.concurrent.Future;

import com.robsonc.solace.data.jpa.service.SolaceWrapper.MessageWithId;

public interface MessageBusWrapper {
	Future<Object> writeMessageToQueue(String queueName, String payload);
	List<MessageWithId> getLatestUnreadMessages(String queueName);
	MessageWithId getLatestUnreadMessage(String queueName);
	void ackMessage(String queueName, String targetMessageId);
	List<MessageWithId> replay(String queueName, Long timestamp);
}
