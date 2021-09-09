package com.robsonc.solace.data.jpa.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.ReplayStartLocation;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.Getter;

@Component
public class SolaceWrapper implements MessageBusWrapper {

	private static final String DEFAULT_VPN = "testservice";

	private ExecutorService executor = Executors.newFixedThreadPool(3);

	@Autowired
	private JCSMPProperties properties;

	@Override
	public Future<Object> writeMessageToQueue(String queueName, String payload, String messageId, String vpn) {
		System.out.println("Going to write message to the queue: " + queueName + " - " + payload);
		Future<Object> future = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
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
			msg.setApplicationMessageId(messageId);

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
	public List<MessageWithId> getLatestUnreadMessages(String queueName, String vpn) {
		System.out.println("VPN IS " + vpn);
		List<MessageWithId> messages = new ArrayList<>();
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
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
					final String payload;
					if (rxMsg instanceof TextMessage) {
						payload = ((TextMessage)rxMsg).getText();
					} else {
						payload = new String(rxMsg.getAttachmentByteBuffer().array());
					}
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
		} finally {
			if (session != null) {
				session.closeSession();
			}
		}
		return messages;
	}

	@Override
	public MessageWithId getLatestUnreadMessage(String queueName, String vpn) {
		MessageWithId lastMessage = null;
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
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
					final String payload;
					if (rxMsg instanceof TextMessage) {
						payload = ((TextMessage)rxMsg).getText();
					} else {
						payload = new String(rxMsg.getAttachmentByteBuffer().array());
					}
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
		} finally {
			if (session != null) {
				session.closeSession();
			}
		}
		return lastMessage;
	}

	@Override
	public MessageWithId getEarliestUnreadMessage(String queueName, String vpn) {
		MessageWithId firstMessage = null;
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
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
				System.out.println("Got message: " + rxMsg);
				if (rxMsg != null) {
					String messageId = rxMsg.getApplicationMessageId();
					System.out.println("message id: " + messageId);
					final String payload;
					if (rxMsg instanceof TextMessage) {
						payload = ((TextMessage)rxMsg).getText();
					} else {
						payload = new String(rxMsg.getAttachmentByteBuffer().array());
					}
					System.out.println("PAYLOAD: " + payload);
					if (payload != null) {
						firstMessage = new MessageWithId(messageId, payload);
					}
				}
			} while (firstMessage == null && rxMsg != null);
		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		} finally {
			if (session != null) {
				session.closeSession();
			}
		}
		return firstMessage;
	}

	@Override
	public void ackMessage(String queueName, String targetMessageId, String vpn) {
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
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
		} finally {
			if (session != null) {
				session.closeSession();
			}
		}
	}

	@Override
	public List<MessageWithId> replay(String queueName, Long timestamp, String vpn) throws Exception {
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
			if (!session.isCapable(CapabilityType.MESSAGE_REPLAY)) {
				throw new Exception("Broker not capable of replaying messages");
			}
			final ReplayStartLocation replayStart;
			if (timestamp != null) {
				Date startDate = new Date(timestamp);
				replayStart = JCSMPFactory.onlyInstance().createReplayStartLocationDate(startDate);
			} else {
				replayStart = JCSMPFactory.onlyInstance().createReplayStartLocationBeginning();
			}
			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
			consumerFlowProperties.setEndpoint(queue);
			// crucial to making sure the message is not acknowledged by the replay
			consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			consumerFlowProperties.setReplayStartLocation(replayStart);

			ReplayFlowEventHandler consumerEventHandler = new ReplayFlowEventHandler();
			List<MessageWithId> list = new ArrayList<>();
			final CountDownLatch latch = new CountDownLatch(1);
			ReplayListener listener = new ReplayListener(latch, list);

			FlowReceiver flowReceiver = session.createFlow(listener, consumerFlowProperties, null, consumerEventHandler);
			flowReceiver.start();
			latch.await(10, TimeUnit.SECONDS);

			return list;
		} catch (InvalidPropertiesException e) {
			e.printStackTrace();
		} finally {
			if (session != null) {
				System.out.println("Closing the session down!!!");
				session.closeSession();
			}
		}
		return null;
	}

	@Data
	private static class ReplayListener implements XMLMessageListener {
		private final CountDownLatch latch;
		private TimerTask timerTask;
		private final List<MessageWithId> messages;

		ReplayListener(CountDownLatch latch, List<MessageWithId> messages) {
			this.latch = latch;
			this.messages = messages;
			Timer timer = new Timer();
			timerTask = new TimerTask(){
				@Override
				public void run() {
					latch.countDown();
				}
			};
			timer.schedule(timerTask, 500);
		}

		@Override
		public void onException(JCSMPException e) {
			System.out.println("Error during replay msg: " + e);
			e.printStackTrace();
			timerTask.cancel();
			// immediately cancel
			timerTask.run();
		}

		@Override
		public void onReceive(BytesXMLMessage msg) {
			timerTask.cancel();
			System.out.println("Received replay msg");

			boolean isRedelivered = msg.getRedelivered();
			if (msg instanceof TextMessage) {
				TextMessage txtMsg = (TextMessage) msg;
				String content = txtMsg.getText();
				MessageWithId messageWithId = new MessageWithId(txtMsg.getApplicationMessageId(), content);
				messages.add(messageWithId);
			} else {
				String content = new String(msg.getAttachmentByteBuffer().array());
				MessageWithId messageWithId = new MessageWithId(msg.getApplicationMessageId(), content);
				messages.add(messageWithId);
			}
			System.out.println("Message: " + messages.get(messages.size() - 1) + " is redelivered? " + isRedelivered);
			// not supported on the EndPoint
			//System.out.println("delivery count: " + msg.getDeliveryCount());
			//System.out.println("delivery mode: " + msg.getDeliveryMode());
			System.out.println("properties: " + msg.getProperties());
			// cancel if no new messages in next second
			Timer timer = new Timer();
			timerTask = new TimerTask(){
				@Override
				public void run() {
					latch.countDown();
				}
			};
			timer.schedule(timerTask, 500);
		}
	}

	private static class ReplayFlowEventHandler implements FlowEventHandler {
		@Getter
		private volatile int replayErrorResponseSubcode = JCSMPErrorResponseSubcodeEx.UNKNOWN;

		@Override
		public void handleEvent(Object source, FlowEventArgs event) {
			System.out.println("Consumer received flow event: " + event);
			if (event.getEvent() == FlowEvent.FLOW_DOWN) {
				if (event.getException() instanceof JCSMPErrorResponseException) {
					JCSMPErrorResponseException ex = (JCSMPErrorResponseException) event.getException();
					replayErrorResponseSubcode = ex.getSubcodeEx();

					switch (replayErrorResponseSubcode) {
						case JCSMPErrorResponseSubcodeEx.REPLAY_STARTED:
						case JCSMPErrorResponseSubcodeEx.REPLAY_FAILED:
						case JCSMPErrorResponseSubcodeEx.REPLAY_CANCELLED:
						case JCSMPErrorResponseSubcodeEx.REPLAY_LOG_MODIFIED:
						case JCSMPErrorResponseSubcodeEx.REPLAY_START_TIME_NOT_AVAILABLE:
						case JCSMPErrorResponseSubcodeEx.REPLAY_MESSAGE_UNAVAILABLE:
						case JCSMPErrorResponseSubcodeEx.REPLAYED_MESSAGE_REJECTED:
							break;
						default:
							break;
					}
				} else {
					System.out.println("Got different type of exception: " + event.getException());
				}
			} else {
				System.out.println("Got replay event: " + event.getEvent());
			}
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
}
