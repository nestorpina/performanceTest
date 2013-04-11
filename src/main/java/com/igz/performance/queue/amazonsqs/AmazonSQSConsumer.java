package com.igz.performance.queue.amazonsqs;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.interfaces.Consumer;

public class AmazonSQSConsumer extends AmazonSQS implements Consumer {

	private static final int PREFETCH_COUNT = 1;
	private String name;
	private DatabaseDAO dao;

	public AmazonSQSConsumer(String name, String queueName, DatabaseDAO dao) {
		super(queueName);
		this.name = name;
		this.dao = dao;
	}
	
	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#getDao()
	 */
	public DatabaseDAO getDao() {
		return dao;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#setDao(com.igz.performance.database.DatabaseDAO)
	 */
	public void setDao(DatabaseDAO dao) {
		this.dao = dao;
	}

	/* (non-Javadoc)
	 * @see com.igz.performance.queues.Consumer#run()
	 */
	public void run() {

		try {
			
			connection = getConnection();

			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            myQueueUrl = connection.createQueue(createQueueRequest).getQueueUrl();
            
			if (debug) {
				System.out.println(" [" + name + "] Consumer started");
			}

			while (!Thread.currentThread().isInterrupted()) {
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl).withMaxNumberOfMessages(PREFETCH_COUNT);
	            List<Message> messages = connection.receiveMessage(receiveMessageRequest).getMessages();

	            for (Message message : messages) {
	                // Process message
	                if (debug) {
	                	System.out.println(" [" + name + "] Received '" + StringUtils.abbreviate(message.getBody(), 100) + "'");
	                }
	                String[] msgSplitted = message.getBody().split("\\"+SEPARATOR);
	                OperationType operation = OperationType.valueOf(msgSplitted[0]);
	                String id = msgSplitted[1];
	                
	                // Execute operation
	                if (operation.equals(OperationType.INSERT)) {
	                	String json = msgSplitted[2];
	                	dao.insert(id, json);
	                } else if (operation.equals(OperationType.SELECT)) {
	                	dao.select(id);
	                } else {
	                	System.err.println("Unssuported message type : " + operation);
	                }
	                
	                // Delete processed a message
	                String messageRecieptHandle = messages.get(0).getReceiptHandle();
	                connection.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
	            }

			}
		} catch (IOException e) {
			e.printStackTrace();
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
