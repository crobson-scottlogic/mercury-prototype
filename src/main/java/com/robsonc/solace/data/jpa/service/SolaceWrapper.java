package com.robsonc.solace.data.jpa.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
public class SolaceWrapper implements MessageBusWrapper {

	private ExecutorService executor = Executors.newFixedThreadPool(3);

	@Autowired
	private JCSMPProperties properties;

	@Override
	public Future<Object> writeMessageToQueue(String queueName, String payload) {
		System.out.println("Going to write message to the queue: " + queueName + " - " + payload);
		Future<Object> future = null;
		try {
			final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();

			final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

			final EndpointProperties endpointProperties = new EndpointProperties();
			endpointProperties.setPermission(EndpointProperties.PERMISSION_CONSUME);
			endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

			session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
			final CountDownLatch latch = new CountDownLatch(1);
			final PubCallback pubCallback = new PubCallback(latch);
			XMLMessageProducer producer = session.getMessageProducer(pubCallback);

			future = executor.submit(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					Object key = pubCallback.getKey();
					if (session != null) {
						session.closeSession();
					}
					return key;
				}
			});

			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			msg.setDeliveryMode(DeliveryMode.PERSISTENT);
			msg.setText(payload);
			UUID randomUUID = UUID.randomUUID();
			msg.setApplicationMessageId(randomUUID.toString());

			final MsgInfo msgCorrelationInfo = new MsgInfo(1);
			msgCorrelationInfo.sessionIndependentMessage = msg;
			msg.setCorrelationKey(msgCorrelationInfo);
			
			producer.send(msg, queue);

		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		}
		return future;
	}

	@Override
	public List<MessageWithId> getLatestUnreadMessages(String queueName) {
		List<MessageWithId> messages = new ArrayList<>();
		try {
			final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
			final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			BrowserProperties browserProperties = new BrowserProperties();
			browserProperties.setEndpoint(queue);
			browserProperties.setTransportWindowSize(1);
			browserProperties.setWaitTimeout(1000);
			Browser browser = session.createBrowser(browserProperties);
			int msgCount = 0;
			BytesXMLMessage rxMsg = null;
			do {
				rxMsg = browser.getNext();
				if (rxMsg != null) {
					msgCount++;
					System.out.println("Browser got message... dumping:");
					System.out.println(rxMsg.dump());
					String messageId = rxMsg.getApplicationMessageId();
					String correlationId = rxMsg.getCorrelationId();
					Long seqNo = rxMsg.getSequenceNumber();
					System.out.println("message id: " + messageId);
					System.out.println("correlation id: " + correlationId);
					System.out.println("sequence id: " + seqNo);
					Long ackMessageId = rxMsg.getAckMessageId();
					System.out.println("ack message id: " + ackMessageId);
					String payload = new String(rxMsg.getAttachmentByteBuffer().array());
					System.out.println("PAYLOAD: " + payload);
					messages.add(new MessageWithId(messageId, payload));
				}
				if (msgCount++ > 100) {
					break;
				}
			} while (rxMsg != null);
		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		}
		return messages;
	}

	@Override
	public MessageWithId getLatestUnreadMessage(String queueName) {
		MessageWithId lastMessage = null;
		try {
			final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
			final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			BrowserProperties browserProperties = new BrowserProperties();
			browserProperties.setEndpoint(queue);
			browserProperties.setTransportWindowSize(1);
			browserProperties.setWaitTimeout(1000);
			Browser browser = session.createBrowser(browserProperties);
			BytesXMLMessage rxMsg = null;
			do {
				rxMsg = browser.getNext();
				if (rxMsg != null) {
					String messageId = rxMsg.getApplicationMessageId();
					System.out.println("message id: " + messageId);
					String payload = new String(rxMsg.getAttachmentByteBuffer().array());
					System.out.println("PAYLOAD: " + payload);
					if (payload != null) {
						lastMessage = new MessageWithId(messageId, payload);
					}
				}
			} while (rxMsg != null);
		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		}
		return lastMessage;
	}

	@Override
	public void ackMessage(String queueName, String targetMessageId) {
		try {
			final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
			final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			BrowserProperties browserProperties = new BrowserProperties();
			browserProperties.setEndpoint(queue);
			browserProperties.setTransportWindowSize(1);
			browserProperties.setWaitTimeout(1000);
			Browser browser = session.createBrowser(browserProperties);
			BytesXMLMessage rxMsg = null;
			do {
				rxMsg = browser.getNext();
				if (rxMsg != null) {
					String messageId = rxMsg.getApplicationMessageId();
					System.out.println("message id: " + messageId);
					// allow messages with null message ID to be acked
					if (Objects.equals(targetMessageId, messageId)) {
						rxMsg.ackMessage();
						// do not break as there is no guarantee that message ID is unique
					}
				}
			} while (rxMsg != null);
		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		}
	}

	private static class PubCallback implements JCSMPStreamingPublishCorrelatingEventHandler {
		private final CountDownLatch latch;
		private Object key = null;
		private PubCallback(CountDownLatch latch) {
			this.latch = latch;
		}
		@Override
		public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
			System.out.println("Error response received: " + key + " error: " + e);
			if (key instanceof MsgInfo) {
				((MsgInfo) key).acked = true;
				System.out.println("Message response (rejected) received for " + key + ", error was " + e);
			}
			this.key = key;
			latch.countDown();
		}
		@Override
		public void responseReceivedEx(Object key) {
			System.out.println("Response received: " + key);
			if (key instanceof MsgInfo) {
				MsgInfo info = (MsgInfo) key;
				info.acked = true;
				info.publishedSuccessfully = true;
				System.out.println("Message response (accepted) received for " + key);
			}
			this.key = key;
			latch.countDown();
		}

		@Override
	    public void handleError(java.lang.String messageID, com.solacesystems.jcsmp.JCSMPException cause, long timestamp) {
	    }
	  
		@Override
	    public void responseReceived(java.lang.String messageID) {
	    }

		public Object getKey() {
			try {
				latch.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return key;
		}
	}

	@Data
	public static class MessageWithId {
		private final String id;
		private final String payload;
	}

	@Override
	public List<MessageWithId> replay(String queueName, Long timestamp) {
		return null;
	}
}
