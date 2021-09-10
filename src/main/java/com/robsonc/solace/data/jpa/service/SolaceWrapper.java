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
import java.util.function.BiPredicate;

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
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class SolaceWrapper implements MessageBusWrapper {

	private static final String DEFAULT_VPN = "testservice";

	private ExecutorService executor = Executors.newFixedThreadPool(3);

	@Autowired
	private JCSMPProperties properties;

	@Override
	public Future<Object> writeMessageToQueue(String queueName, String payload, String messageId, String vpn) {
		log.info("Going to write message to the queue: {} - {}", queueName, payload);
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

			// TODO: test if this is needed when you know that the queue already exists?
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

		} catch (JCSMPException e) {
			log.error("Error encountered when writing message to the queue", e);
		}
		return future;
	}

	private List<MessageWithId> peekUnackedMessages(String queueName, String vpn, BiPredicate<BytesXMLMessage, MessageWithId> loopCondition) {
		List<MessageWithId> messages = new ArrayList<>();
		JCSMPSession session = null;
		try {
			if (vpn == null) {
				vpn = DEFAULT_VPN;
			}
			properties.setProperty(JCSMPProperties.VPN_NAME, vpn);
			session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();
			Browser browser = createBrowser(queueName, session);
			int msgCount = 0;
			BytesXMLMessage rxMsg = null;
			MessageWithId currentMessage = null;
			do {
				rxMsg = browser.getNext();
				if (rxMsg != null) {
					msgCount++;
					String messageId = rxMsg.getApplicationMessageId();
					final String payload;
					if (rxMsg instanceof TextMessage) {
						payload = ((TextMessage)rxMsg).getText();
					} else {
						payload = new String(rxMsg.getAttachmentByteBuffer().array());
					}
					log.info("message ID: {}, PAYLOAD: {}", payload);
					currentMessage = new MessageWithId(messageId, payload);
					messages.add(currentMessage);
				}
				if (msgCount++ > 100) {
					break;
				}
			} while (loopCondition.test(rxMsg, currentMessage));
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
	public List<MessageWithId> peekAllUnackedMessages(String queueName, String vpn) {
		log.info("Going to peek all unacked messages in queue: {}", queueName);
		return peekUnackedMessages(queueName, vpn, (nextMessage, __) -> nextMessage != null);
	}

	@Override
	public MessageWithId peekLatestUnackedMessage(String queueName, String vpn) {
		log.info("Going to peek at last unacked message in queue: {}", queueName);
		List<MessageWithId> allUnackedMessages = peekUnackedMessages(queueName, vpn, (nextMessage, __) -> nextMessage != null);
		if (allUnackedMessages != null && allUnackedMessages.size() > 0) {
			return allUnackedMessages.get(allUnackedMessages.size() - 1);
		} else {
			return null;
		}
	}

	@Override
	public MessageWithId peekEarliestUnackedMessage(String queueName, String vpn) {
		log.info("Going to peek at earliest unacked message in queue: {}", queueName);
		// slightly different predicate should short circuit the method so that only the first message with a non-null payload is fetched
		List<MessageWithId> unackedMessages = peekUnackedMessages(queueName, vpn, (nextMessage, currentMessage) -> nextMessage != null && currentMessage == null);
		if (unackedMessages != null && unackedMessages.size() > 0) {
			log.info("Messages fetched: {}", unackedMessages.size());
			return unackedMessages.get(0);
		}
		return null;
	}

	private Browser createBrowser(String queueName, JCSMPSession session) throws JCSMPException {
			log.info("Queue name is ", queueName);
			final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			BrowserProperties browserProperties = new BrowserProperties();
			browserProperties.setEndpoint(queue);
			browserProperties.setTransportWindowSize(1);
			browserProperties.setWaitTimeout(1000);
			return session.createBrowser(browserProperties);
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
			Browser browser = createBrowser(queueName, session);
			BytesXMLMessage rxMsg = null;
			do {
				rxMsg = browser.getNext();
				if (rxMsg != null) {
					String messageId = rxMsg.getApplicationMessageId();
					log.info("message id: " + messageId);
					// allow messages with null message ID to be acked
					if (Objects.equals(targetMessageId, messageId)) {
						rxMsg.ackMessage();
						// do not break as there is no guarantee that message ID is unique
					}
				}
			} while (rxMsg != null);
		} catch (JCSMPException e) {
			log.error("Error encountered when sending ack", e);
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
			final ReplayStartLocation replayStart = timestamp == null ?
				JCSMPFactory.onlyInstance().createReplayStartLocationBeginning() :
				JCSMPFactory.onlyInstance().createReplayStartLocationDate(new Date(timestamp));

			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
			consumerFlowProperties.setEndpoint(queue);
			// crucial to making sure the message is not acknowledged by the replay
			consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			consumerFlowProperties.setReplayStartLocation(replayStart);

			ReplayFlowEventHandler consumerEventHandler = new ReplayFlowEventHandler();
			final CountDownLatch latch = new CountDownLatch(1);
			ReplayListener listener = new ReplayListener(latch);

			FlowReceiver flowReceiver = session.createFlow(listener, consumerFlowProperties, null, consumerEventHandler);
			flowReceiver.start();
			latch.await(10, TimeUnit.SECONDS);

			return listener.getMessages();
		} finally {
			if (session != null) {
				session.closeSession();
			}
		}
	}

	@Data
	private static class ReplayListener implements XMLMessageListener {
		private final CountDownLatch latch;
		private TimerTask timerTask;
		@Getter
		private final List<MessageWithId> messages = new ArrayList<>();

		ReplayListener(CountDownLatch latch) {
			this.latch = latch;
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
			log.error("Error during replay msg", e);
			timerTask.cancel();
			// immediately cancel
			timerTask.run();
		}

		@Override
		public void onReceive(BytesXMLMessage msg) {
			timerTask.cancel();
			log.info("Received replay msg");

			final String content;
			if (msg instanceof TextMessage) {
				TextMessage txtMsg = (TextMessage) msg;
				content = txtMsg.getText();
			} else {
				content = new String(msg.getAttachmentByteBuffer().array());
			}
			MessageWithId messageWithId = new MessageWithId(msg.getApplicationMessageId(), content);
			messages.add(messageWithId);

			// reset the timer
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
			log.info("Consumer received flow event: " + event);
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
					log.info("Got different type of exception: " + event.getException());
				}
			} else {
				log.info("Got replay event: " + event.getEvent());
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
			log.error("Error response received: {}", key, e);
			if (key instanceof MsgInfo) {
				((MsgInfo) key).acked = true;
			}
			this.key = key;
			latch.countDown();
		}

		@Override
		public void responseReceivedEx(Object key) {
			log.info("Response received: {}", key);
			if (key instanceof MsgInfo) {
				MsgInfo info = (MsgInfo) key;
				info.acked = true;
				info.publishedSuccessfully = true;
				log.info("Message response (accepted) received for {}", key);
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
