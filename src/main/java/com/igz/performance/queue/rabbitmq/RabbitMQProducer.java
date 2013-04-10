package com.igz.performance.queue.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;

import com.igz.performance.queue.interfaces.Producer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQProducer extends RabbitMQ implements Callable<List<String>>, Producer {

	private String name;
	private int numberOfRequests;
	private OperationType operation;
	private String json;
	private List<String> idsToSelect;

	private Channel channel;
	
	public RabbitMQProducer(String name, String queueName, int numberOfRequests) {
		super(queueName);
		this.name = name;
		this.numberOfRequests = numberOfRequests;
	}

	public List<String> getIdsToSelect() {
		return idsToSelect;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Producer#setIdsToSelect(java.util.List)
	 */
	public void setIdsToSelect(List<String> idsToSelect) {
		this.idsToSelect = idsToSelect;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Producer#getOperation()
	 */
	public OperationType getOperation() {
		return operation;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Producer#setOperation(com.igz.performance.queues.RabbitMQ.OperationType)
	 */
	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public String getJson() {
		return json;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Producer#setJson(java.lang.String)
	 */
	public void setJson(String json) {
		this.json = json;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Producer#call()
	 */
	public List<String> call() {

		List<String> ids = new ArrayList<String>();

		try {
			connection = getConnection();
			channel = connection.createChannel();

			channel.queueDeclare(queueName, QUEUE_CONFIG_DURABLE, QUEUE_CONFIG_EXCLUSIVE, QUEUE_CONFIG_AUTODELETE, null);

			if (operation == OperationType.INSERT) {
				ids = sendInserts();
			} else if (operation == OperationType.SELECT) {
				ids = sendSelects();
			}

			channel.close();
			connection.close();

			return ids;
		} catch (Throwable ex) {
			ex.printStackTrace();
		}
		return ids;
	}

	private List<String> sendSelects() throws IOException {
		List<String> ids = new ArrayList<String>();
		for (String id : idsToSelect) {
			String message = operation + SEPARATOR + id;
			channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			if (debug) {
				System.out.println(" [" + name + "] Sent '" + message + "'");
			}
		}
		return ids;
	}

	private List<String> sendInserts() throws IOException {
		List<String> ids = new ArrayList<String>();
		for (int i = 0; i < numberOfRequests; i++) {
			String id = UUID.randomUUID().toString();
			ids.add(id);
			String message = operation + SEPARATOR + id + SEPARATOR + json;
			channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
			if (debug) {
				System.out.println(" [" + name + "] Sent '" + StringUtils.abbreviate(message, 100) + "'");
			}
		}
		return ids;
	}

}