package com.robsonc.solace.data.jpa.service;

import java.util.List;
import java.util.concurrent.Future;

import com.robsonc.solace.data.jpa.service.SolaceWrapper.MessageWithId;

public interface MessageBusWrapper {
	Future<Object> writeMessageToQueue(String queueName, String payload, String messageId, String vpn);
	List<MessageWithId> peekAllUnackedMessages(String queueName, String vpn);
	MessageWithId peekLatestUnackedMessage(String queueName, String vpn);
	MessageWithId peekEarliestUnackedMessage(String queueName, String vpn);
	void ackMessage(String queueName, String targetMessageId, String vpn);
	List<MessageWithId> replay(String queueName, Long timestamp, String vpn) throws Exception;
}
