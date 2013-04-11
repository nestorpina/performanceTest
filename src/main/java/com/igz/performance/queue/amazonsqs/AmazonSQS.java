package com.igz.performance.queue.amazonsqs;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.queue.AbstractQueue;
import com.igz.performance.queue.interfaces.Consumer;
import com.igz.performance.queue.interfaces.Producer;
import com.igz.performance.queue.interfaces.Queue;

public class AmazonSQS extends AbstractQueue implements Queue {

	protected com.amazonaws.services.sqs.AmazonSQS connection;
	protected String myQueueUrl;
	protected PropertiesConfiguration configuration;

	public AmazonSQS(String queueName) {
		super(queueName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Queue#getPendingMessages()
	 */
	public int getPendingMessages() {
		int messageCount = -1;
		try {
			connection = getConnection();
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            myQueueUrl = connection.createQueue(createQueueRequest).getQueueUrl();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(myQueueUrl).withAttributeNames("ApproximateNumberOfMessages");
		GetQueueAttributesResult queueAttributes = connection.getQueueAttributes(getQueueAttributesRequest);
		Map<String, String> attributes = queueAttributes.getAttributes();
		for (String key : attributes.keySet()) {
			if(key.equals("ApproximateNumberOfMessages")) {
				messageCount = Integer.valueOf(attributes.get(key));
				break;
			}
		}

		return messageCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.igz.performance.queues.Queue#deleteQueue()
	 */
	public void deleteQueue() {
		if (debug) {
			System.out.println("Deleting queue: " + queueName);
		}
		try {
			connection.deleteQueue(new DeleteQueueRequest(myQueueUrl));
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SQS, such as not "
							+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	protected com.amazonaws.services.sqs.AmazonSQS getConnection() throws IOException {
		if(connection== null) {
			try {
				configuration = new PropertiesConfiguration("application.properties");
			} catch (ConfigurationException e) {
				throw new RuntimeException(e);
			}
			String accessKey = configuration.getString("accessKey");
			String secretKey = configuration.getString("secretKey");
	
			connection = new AmazonSQSClient(new BasicAWSCredentials(accessKey,secretKey));
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			connection.setRegion(usWest2);
		}
		return connection;
	}

	public Consumer createConsumer(String name, DatabaseDAO databaseDAO) {
		return new AmazonSQSConsumer(name, queueName, databaseDAO);

	}

	public Producer createProducer(String name, int numberOfRequests) {
		return new AmazonSQSProducer(name, queueName, numberOfRequests);
	}
}
