package com.robsonc.solace.data.jpa.service;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
}
