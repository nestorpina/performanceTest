package com.igz.performance.queue.amazonsqs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.igz.performance.queue.interfaces.Producer;

public class AmazonSQSProducer extends AmazonSQS implements Callable<List<String>>, Producer {

	private String name;
	private int numberOfRequests;
	private OperationType operation;
	private String json;
	private List<String> idsToSelect;

	public AmazonSQSProducer(String name, String queueName, int numberOfRequests) {
		super(queueName);
		this.name = name;
		this.numberOfRequests = numberOfRequests;
	}

	public List<String> getIdsToSelect() {
		return idsToSelect;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Producer#setIdsToSelect(java.util.List)
	 */
	public void setIdsToSelect(List<String> idsToSelect) {
		this.idsToSelect = idsToSelect;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Producer#getOperation()
	 */
	public OperationType getOperation() {
		return operation;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Producer#setOperation(com.igz.performance.queues.RabbitMQ.OperationType)
	 */
	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public String getJson() {
		return json;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Producer#setJson(java.lang.String)
	 */
	public void setJson(String json) {
		this.json = json;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Producer#call()
	 */
	public List<String> call() {

		List<String> ids = new ArrayList<String>();

		try {
			connection = getConnection();

			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			myQueueUrl = connection.createQueue(createQueueRequest).getQueueUrl();

			if (operation == OperationType.INSERT) {
				ids = sendInserts();
			} else if (operation == OperationType.SELECT) {
				ids = sendSelects();
			}

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
			send(message);
		}
		return ids;
	}

	private List<String> sendInserts() throws IOException {
		List<String> ids = new ArrayList<String>();
		for (int i = 0; i < numberOfRequests; i++) {
			String id = UUID.randomUUID().toString();
			ids.add(id);
			String message = operation + SEPARATOR + id + SEPARATOR + json;
			send(message);
		}
		return ids;
	}

	private void send(String message) {
		try {
			connection.sendMessage(new SendMessageRequest(myQueueUrl, message));
			if (debug) {
				System.out.println(" [" + name + "] Sent '" + StringUtils.abbreviate(message, 100) + "'");
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			throw ase;
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			throw ace;
		}
	}

}