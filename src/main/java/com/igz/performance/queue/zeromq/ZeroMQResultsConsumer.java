package com.igz.performance.queue.zeromq;

import org.zeromq.ZMQ;

/**
 * This class binds a ZeroMQ socket that receives OK messages from the consumers when they have 
 * finished processing their tasks. We count the OK messages received to determine the number
 * of results that are already processed. This way we expose this data to ZeroMq.getPendingMessages
 * in order to wait in PerformamceTest for all the consumers to finish, and control timings
 * 
 * TODO : find a better way to access the pending results than to have static variables...
 * 
 * @author npina
 *
 */
public class ZeroMQResultsConsumer extends ZeroMq implements Runnable {


	public ZeroMQResultsConsumer(int expectedResults) {
		super("");
		ZeroMQResultsConsumer.expectedResults = expectedResults;
	}

	private static int processedResults;
	private static int expectedResults;
	
	public static int getProcessedResults() {
		return processedResults;
	}

	public static void setProcessedResults(int results) {
		ZeroMQResultsConsumer.processedResults = results;
	}

	public static int getExpectedResults() {
		return expectedResults;
	}

	public static void setExpectedResults(int expectedResults) {
		ZeroMQResultsConsumer.expectedResults = expectedResults;
	}

	public void run() {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket consumer = context.socket(ZMQ.PULL);
		consumer.bind("tcp://*:5557");

		if (debug) {
			System.out.println(" [RESULTS] Consumer started");
		}

		while (!Thread.currentThread().isInterrupted()) {

			// Use trim to remove the tailing '0' character
			String message = consumer.recvStr(0).trim();

			processedResults++;

			if (debug) {
				System.out.println(String.format(" [RESULTS] Received '%s' (%d/%d) ", message, processedResults, expectedResults));
			}
			
		}

		consumer.close();
		context.term();
	}

}
