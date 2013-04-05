package com.igz.performance.queues;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.StopWatch;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.database.MongoDBDAO;
import com.igz.performance.database.MysqlDAO;
import com.igz.performance.database.RedisDAO;

public class RabbitMQTest {
	
	public static boolean debug = false;

	private static DatabaseDAO dao;
	private static ArrayList<RabbitMQConsumer> consumers;
	private static ArrayList<Callable<List<String>>> producers;

	private static int requestPerProducer;

	private final static String JSON = "{ 'LoadTime': 3, 'BackgroundImage': 'http://desprolamoderna.antena3.com/aplication/eIxlnfA8LPYl.png', 'DebugInfo': 1, 'RefreshInterval': 90, 'TimeTriggerMin': 150401, 'TimeTriggerMax': 150430, 'CodewordLatency': 10.08, 'ApplicationTitle': 'Prototipo Dev', 'facebookUrl': 'https:\\/\\/m.facebook.com', 'twitterUrl': 'https:\\/\\/m.twitter.com', 'Modules': [ { 'Id': 2, 'Name': 'Secciones', 'Programs': [ ] } , { 'Id': 3, 'Name': 'Participación', 'Contests': [ { 'Id': 20, 'Name': 'prueba concurso', 'Category': 'concurso', 'ImageIcon': 'http://desprolamoderna.antena3.com/contest/ZDH2Pg4P3F2t.png', 'ImageDetailed': 'http://desprolamoderna.antena3.com/contest/oezJ98JV1rLD.png', 'Description': 'oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu', 'Url': 'http:\\/\\/www.google.es', 'Questions': [ { 'Id': 44, 'Question': '¿Tu compañia de seguros reinvierte gran parte de sus beneficios en ti?', 'Time': 24, 'Background': 'http://desprolamoderna.antena3.com/pregunta/4YvyekkXnEcn.png', 'Answers': [ { 'Id': 110, 'Text': 'No No No', 'Correct': 1 } , { 'Id': 111, 'Text': 'Si', 'Correct': 0 } ] } , { 'Id': 45, 'Question': '¿Pregunta de prueba 2?', 'Time': 35, 'Background': 'http://desprolamoderna.antena3.com/pregunta/CbhQEURIpe9J.png', 'Answers': [ { 'Id': 112, 'Text': 'rpta uno', 'Correct': 0 } , { 'Id': 113, 'Text': 'rpta dos', 'Correct': 0 } , { 'Id': 114, 'Text': 'rpta 3', 'Correct': 1 } ] } , { 'Id': 46, 'Question': '¿Cargará la imagen de la campaña?', 'Time': 50, 'Answers': [ { 'Id': 115, 'Text': 'Sí', 'Correct': 0 } , { 'Id': 116, 'Text': 'No', 'Correct': 0 } , { 'Id': 117, 'Text': 'Tal vez', 'Correct': 0 } ] } ] } ] } , { 'Id': 1, 'Name': 'Guardado' } ] , 'Campaigns' : [ { 'Id': 272, 'Name': 'prueba pregunta', 'Codewords': [150441,150442], 'Background': 'http://desprolamoderna.antena3.com/imagen/9vrL8gMdTdHx.png', 'BackgroundMD5': '172c1847902aee8137be53a9bfbe458', 'Events': [ { 'Id': 566, 'Name': 'pregunta mutua', 'Description': 'Esta es la descripcion de facebook', 'QuestionId': 44, 'Start': 11.0, 'End': 20.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 567, 'Name': 'Lanza segunda pregunta', 'Description': 'pregunta dos', 'QuestionId': 45, 'Start': 25.0, 'End': 41.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 568, 'Name': 'Tercera pregunta', 'Description': 'pregunta 3', 'QuestionId': 46, 'Start': 45.0, 'End': 59.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 70.0, 'End': 170.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } , { 'Id': 37, 'Name': 'demoAtrapa1millonCorta', 'Codewords': [150435,150436], 'Background': 'http://agm28.blob.core.windows.net/imagen/eceNhoG1Acsk.png', 'Events': [ { 'Id': 63, 'Name': 'Eleccion Tema Seres', 'Start': 36.0, 'End': 50.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/sry7UoXYeqiF.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 64, 'Name': 'Resp1 ewoks', 'Start': 52.0, 'End': 54.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/9ph3YHqs2OFq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 65, 'Name': 'Resp 2 replicantes', 'Start': 56.0, 'End': 60.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/rYx8aspGR5b5.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 66, 'Name': 'Resp3 transformers', 'Start': 62.0, 'End': 64.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/eV9HD4xwh8Eq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 67, 'Name': 'Resp4 avatares', 'Start': 65.0, 'End': 84.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/ppTong1SRzKd.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 74, 'Name': 'Pregunta1imagen', 'Start': 86.0, 'End': 90.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/Hd7XREgeuOTQ.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 69, 'Name': 'Pregunta1', 'QuestionId': 12, 'Start': 90.0, 'End': 224.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 113, 'Name': ' videoImagenio', 'Start': 1500.0, 'End': 2500.0, 'EventType': 2, 'FileTag': 'http://agm28.blob.core.windows.net/content/fxAZPIpy4VMC.mp4', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 254.0, 'End': 354.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } ] }";
	private final static String QUEUE_NAME = "test_queue"+System.currentTimeMillis();

	private final static int CONSUMER_COUNT = 10;
	private final static int PRODUCER_COUNT = 1;
	private final static int ITEMS = 100000;
	
	public enum OperationType { INSERT, SELECT }

	private static DatabaseDAO getDatabaseDAO() {
		return new RedisDAO();
	}

	public static void main(String[] args) throws Throwable {
		// Ids of inserted items
		List<Object> ids = new ArrayList<Object>();
		
		requestPerProducer = ITEMS / PRODUCER_COUNT;
		
		connectAndCleanDB();
		
		startConsumers();
		
		Thread.sleep(1000); // Wait a little for consumers to become ready

		prepareProducers();
		
		// Start timing and execute producers
		StopWatch stopwatch = new StopWatch();
    	stopwatch.start();
    	ExecutorService executorService = Executors.newFixedThreadPool(PRODUCER_COUNT);
		List<Future<List<String>>> futures = executorService.invokeAll((List<Callable<List<String>>>)producers);

		// Wait for all producers to finish, and get the ids generated
		for (Future<List<String>> future : futures) {
			ids.addAll(future.get());
		}

		waitUntilEmptyQueue();
		stopwatch.suspend();

		int count = checkInsertedCount();
		
		System.out.println(String.format("Inserted %d (%d producers / %d consumers) in %s", count, PRODUCER_COUNT, CONSUMER_COUNT, stopwatch));

		// Change producers to execute select operation, to retrieve all items, and set ids to select
		for (int i = 0; i < PRODUCER_COUNT; i++) {
		    List<Object> subList = ids.subList(i*requestPerProducer, (i+1)*requestPerProducer);
			RabbitMQProducer producer = (RabbitMQProducer) producers.get(i);
			producer.setOperation(OperationType.SELECT);
			producer.setIdsToSelect(subList);
		}
		// Start timing again and execute selects
		stopwatch.reset();
		stopwatch.start();
		executorService.invokeAll(producers);
		
		// Wait for all producers to finish, and get the ids generated
		for (Future<List<String>> future : futures) {
			future.get();
		}
		waitUntilEmptyQueue();
		stopwatch.suspend();
		
		System.out.println(String.format("Retrieved %d (%d producers / %d consumers) in %s", ids.size(), PRODUCER_COUNT, CONSUMER_COUNT, stopwatch));

		cleanAndcloseDBConnections();
		
		deleteQueue();
		// Exit, to close threads
		System.exit(0);
	}

	private static void deleteQueue() throws InterruptedException {
		RabbitMQ rabbitMQ = new RabbitMQ(QUEUE_NAME);
		rabbitMQ.setDebug(debug);
		rabbitMQ.deleteQueue();
	}

	private static void cleanAndcloseDBConnections() throws Throwable {
		// Clean database and close connections
		dao.removeAll();
		dao.close();
		for (RabbitMQConsumer consumer : consumers) {
			consumer.getDao().close();
		}
	}

	private static int checkInsertedCount() {
		int count = dao.count();
		if (count != ITEMS) {
			System.err.println("ERROR: expected " + ITEMS + ", found " + count);
		}
		return count;
	}

	private static void prepareProducers() {
		producers = new ArrayList<Callable<List<String>>>();
		for (int i = 0; i < PRODUCER_COUNT; i++) {
			RabbitMQProducer producer = new RabbitMQProducer("P" + i, QUEUE_NAME, requestPerProducer);
			producer.setOperation(OperationType.INSERT);
			producer.setDebug(debug);
			producer.setJson(JSON);
			producers.add(producer);
		}
	}

	private static void startConsumers() {
		consumers = new ArrayList<RabbitMQConsumer>();
		for (int i = 0; i < CONSUMER_COUNT; i++) {
			RabbitMQConsumer consumer = new RabbitMQConsumer("C" + i, QUEUE_NAME, getDatabaseDAO());
			consumer.setDebug(debug);
			Thread thread = new Thread(consumer);
			thread.start();
			consumers.add(consumer);
		}
	}

	private static void connectAndCleanDB() {
		dao = getDatabaseDAO();
		dao.init();
		dao.removeAll();
	}

	// TODO: Maybe there are messages being processed even when the queue is empty. How do we check that?
	private static void waitUntilEmptyQueue() throws InterruptedException {
		RabbitMQ rabbitMQ = new RabbitMQ(QUEUE_NAME);
		rabbitMQ.setDebug(debug);
		int pendingMessages = -1;
		while (pendingMessages != 0) {
			Thread.sleep(10);
			pendingMessages = rabbitMQ.getPendingMessages();
			if(debug) {
				System.out.println("Pending: " + pendingMessages);
			}
		}
	}

}
