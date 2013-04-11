package com.igz.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.time.StopWatch;

import com.igz.performance.database.DatabaseDAO;
import com.igz.performance.database.DatabaseFactory;
import com.igz.performance.options.Options;
import com.igz.performance.options.OptionsParser;
import com.igz.performance.queue.AbstractQueue.OperationType;
import com.igz.performance.queue.QueueFactory;
import com.igz.performance.queue.QueueFactory.QueueType;
import com.igz.performance.queue.interfaces.Consumer;
import com.igz.performance.queue.interfaces.Producer;
import com.igz.performance.queue.interfaces.Queue;
import com.igz.performance.queue.none.DirectProducer;
import com.igz.performance.queue.zeromq.ZeroMQResultsConsumer;

/**
 * Performance testing of INSERTS and SELECTS of a json in different Databases, and using differente queue systems
 * 
 * @author npina
 * 
 */
public class PerformanceTest {

	private static ZeroMQResultsConsumer resultsConsumer;

	private static Options options;

	private static Queue queue;

	private final static String JSON = "{ 'LoadTime': 3, 'BackgroundImage': 'http://desprolamoderna.antena3.com/aplication/eIxlnfA8LPYl.png', 'DebugInfo': 1, 'RefreshInterval': 90, 'TimeTriggerMin': 150401, 'TimeTriggerMax': 150430, 'CodewordLatency': 10.08, 'ApplicationTitle': 'Prototipo Dev', 'facebookUrl': 'https:\\/\\/m.facebook.com', 'twitterUrl': 'https:\\/\\/m.twitter.com', 'Modules': [ { 'Id': 2, 'Name': 'Secciones', 'Programs': [ ] } , { 'Id': 3, 'Name': 'Participación', 'Contests': [ { 'Id': 20, 'Name': 'prueba concurso', 'Category': 'concurso', 'ImageIcon': 'http://desprolamoderna.antena3.com/contest/ZDH2Pg4P3F2t.png', 'ImageDetailed': 'http://desprolamoderna.antena3.com/contest/oezJ98JV1rLD.png', 'Description': 'oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu', 'Url': 'http:\\/\\/www.google.es', 'Questions': [ { 'Id': 44, 'Question': '¿Tu compañia de seguros reinvierte gran parte de sus beneficios en ti?', 'Time': 24, 'Background': 'http://desprolamoderna.antena3.com/pregunta/4YvyekkXnEcn.png', 'Answers': [ { 'Id': 110, 'Text': 'No No No', 'Correct': 1 } , { 'Id': 111, 'Text': 'Si', 'Correct': 0 } ] } , { 'Id': 45, 'Question': '¿Pregunta de prueba 2?', 'Time': 35, 'Background': 'http://desprolamoderna.antena3.com/pregunta/CbhQEURIpe9J.png', 'Answers': [ { 'Id': 112, 'Text': 'rpta uno', 'Correct': 0 } , { 'Id': 113, 'Text': 'rpta dos', 'Correct': 0 } , { 'Id': 114, 'Text': 'rpta 3', 'Correct': 1 } ] } , { 'Id': 46, 'Question': '¿Cargará la imagen de la campaña?', 'Time': 50, 'Answers': [ { 'Id': 115, 'Text': 'Sí', 'Correct': 0 } , { 'Id': 116, 'Text': 'No', 'Correct': 0 } , { 'Id': 117, 'Text': 'Tal vez', 'Correct': 0 } ] } ] } ] } , { 'Id': 1, 'Name': 'Guardado' } ] , 'Campaigns' : [ { 'Id': 272, 'Name': 'prueba pregunta', 'Codewords': [150441,150442], 'Background': 'http://desprolamoderna.antena3.com/imagen/9vrL8gMdTdHx.png', 'BackgroundMD5': '172c1847902aee8137be53a9bfbe458', 'Events': [ { 'Id': 566, 'Name': 'pregunta mutua', 'Description': 'Esta es la descripcion de facebook', 'QuestionId': 44, 'Start': 11.0, 'End': 20.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 567, 'Name': 'Lanza segunda pregunta', 'Description': 'pregunta dos', 'QuestionId': 45, 'Start': 25.0, 'End': 41.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 568, 'Name': 'Tercera pregunta', 'Description': 'pregunta 3', 'QuestionId': 46, 'Start': 45.0, 'End': 59.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 70.0, 'End': 170.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } , { 'Id': 37, 'Name': 'demoAtrapa1millonCorta', 'Codewords': [150435,150436], 'Background': 'http://agm28.blob.core.windows.net/imagen/eceNhoG1Acsk.png', 'Events': [ { 'Id': 63, 'Name': 'Eleccion Tema Seres', 'Start': 36.0, 'End': 50.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/sry7UoXYeqiF.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 64, 'Name': 'Resp1 ewoks', 'Start': 52.0, 'End': 54.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/9ph3YHqs2OFq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 65, 'Name': 'Resp 2 replicantes', 'Start': 56.0, 'End': 60.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/rYx8aspGR5b5.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 66, 'Name': 'Resp3 transformers', 'Start': 62.0, 'End': 64.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/eV9HD4xwh8Eq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 67, 'Name': 'Resp4 avatares', 'Start': 65.0, 'End': 84.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/ppTong1SRzKd.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 74, 'Name': 'Pregunta1imagen', 'Start': 86.0, 'End': 90.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/Hd7XREgeuOTQ.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 69, 'Name': 'Pregunta1', 'QuestionId': 12, 'Start': 90.0, 'End': 224.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 113, 'Name': ' videoImagenio', 'Start': 1500.0, 'End': 2500.0, 'EventType': 2, 'FileTag': 'http://agm28.blob.core.windows.net/content/fxAZPIpy4VMC.mp4', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 254.0, 'End': 354.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } ] }";
	private final static String QUEUE_NAME = "test_queue" + System.currentTimeMillis();

	private static int producerWorkers = 1;

	public static void main(String[] args) {
		
		OptionsParser optionsParser = new OptionsParser();
		try {
			options = optionsParser.parseCommandLineOptions(args);
		} catch (ParseException e1) {
			System.exit(0);
		}
		if (options.showHelp()) {
			optionsParser.printHelp();
			System.exit(0);
		}

		ArrayList<Consumer> consumers = null;
		DatabaseDAO dao = null;

		// Print summary if not in csv output mode
		if (!options.isOutputCsv()) {
			System.out.println(String.format("Using DATABASE: %s, QUEUE: %s, WORKERS: %d, EVENTS: %d", options.getDatabaseType(),
					options.getQueueType(), options.getNumWorkers(), options.getNumEvents()));
		}

		try {

			dao = connectAndCleanDB();

			if (options.getQueueType() != QueueType.NONE) {
				consumers = startConsumers();
				Thread.sleep(3000); // Wait a little for consumers to become ready
			} else {
				// If we are not using a queue, the num of workers are the producers instead of the consumers
				producerWorkers = options.getNumWorkers();
			}

			ArrayList<Callable<List<String>>> producers = prepareProducersForInsert();

			List<String> ids = testInserts(producers, dao);

			// restart counter of results for ZeroMQ
			if (options.getQueueType() == QueueType.ZEROMQ) {
				ZeroMQResultsConsumer.setProcessedResults(0);
			}

			producers = prepareProducersForSelect(ids);

			testSelects(producers, dao, ids);

			System.exit(0); // Exit, to close threads
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			cleanAndcloseDBConnections(consumers, dao);
			deleteQueue();
		}
	}

	/**
	 * Creates a new DatabaseDao instance depending on the options
	 * 
	 * @return DatabaseDAO
	 */
	private static DatabaseDAO createDatabaseDAO() {
		DatabaseDAO dao = DatabaseFactory.createDatabase(options.getDatabaseType());
		dao.init();
		return dao;
	}

	/**
	 * Creates a new Producer instance depending on the options
	 * 
	 * @param requestPerProducer
	 *            - number of requests that this producer will execute
	 * @param i
	 *            - number to identify the producer in logs, it has no other function
	 * 
	 * @return Producer
	 */
	private static Producer cretateProducer(int requestPerProducer, int i) {
		Producer producer = getQueue().createProducer("P" + i, requestPerProducer);
		producer.setDebug(options.debug());
		return producer;
	}

	/**
	 * Creates a new Consumer instance depending on the options
	 * 
	 * @param i
	 *            - number to identify the consumer in logs, it has no other function
	 * 
	 * @return Consumer
	 */
	private static Consumer createConsumer(int i) {
		Consumer consumer = getQueue().createConsumer("C" + i, createDatabaseDAO());
		consumer.setDebug(options.debug());
		return consumer;
	}

	/**
	 * Creates a new Queue Instance, used for instantiating Producers and Consumers, and for general queries, like getPendingMessages, etc
	 * 
	 * @return Queue
	 */
	private static Queue getQueue() {
		if (queue == null) {
			queue = QueueFactory.createDatabase(options.getQueueType(), QUEUE_NAME);
			queue.setDebug(options.debug());
		}
		return queue;
	}

	/**
	 * Test selects from database using the defined producers
	 * 
	 * @param producers
	 * @param dao
	 * @param ids
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static void testSelects(ArrayList<Callable<List<String>>> producers, DatabaseDAO dao, List<String> ids)
			throws InterruptedException, ExecutionException {

		StopWatch stopwatch = new StopWatch();
		stopwatch.start();
		executeInThreads(producers);
		waitUntilEmptyQueue();
		stopwatch.suspend();

		printTime("Retrieved", ids.size(), stopwatch);

	}

	/**
	 * Test inserts in database using the defined producers
	 * 
	 * @param producers
	 * @param dao
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static List<String> testInserts(ArrayList<Callable<List<String>>> producers, DatabaseDAO dao) throws InterruptedException,
			ExecutionException {

		StopWatch stopwatch = new StopWatch();
		stopwatch.start();
		List<String> ids = executeInThreads(producers);

		waitUntilEmptyQueue();
		int completedInserts = waitUntilAllItemsInserted(dao);
		stopwatch.suspend();

		printTime("Inserted", completedInserts, stopwatch);

		return ids;

	}

	/**
	 * Helper method to print elapsed time to perform the specified operations, with info about producers, consumers and number of inserts
	 * 
	 * @param operation
	 *            - description of the operation
	 * @param total
	 *            - number of events
	 * @param stopwatch
	 *            - timer
	 */
	private static void printTime(String operation, int total, StopWatch stopwatch) {
		String producerAndConsumersDesc;
		if (options.getQueueType() == QueueType.NONE) {
			producerAndConsumersDesc = String.format("%d producers", options.getNumWorkers());
		} else {
			producerAndConsumersDesc = String.format("%d producers / %d consumers", producerWorkers, options.getNumWorkers());
		}
		if (options.isOutputCsv()) {
			System.out.println(String.format("%s,%s,Java,%s %d,%d,,%s", options.getDatabaseType(), options.getQueueType(), operation,
					total, stopwatch.getTime(), producerAndConsumersDesc));

		} else {
			System.out.println(String.format("%s %d (%s) in %s", operation, total, producerAndConsumersDesc, stopwatch));
		}
	}

	/**
	 * Blocking method, doing a count on the database each 100ms until all the inserts are executed
	 * 
	 * @throws InterruptedException
	 * @return number of inserts (items found on db)
	 */
	private static int waitUntilAllItemsInserted(DatabaseDAO dao) throws InterruptedException {
		int completedInserts = dao.count();
		int i = 0;
		while (completedInserts < options.getNumEvents()) {
			Thread.sleep(100);
			completedInserts = dao.count();
			// We print status each 100ms in debug, but also each 1sec even without debug
			if (options.debug() || i % 10 == 0) {
				System.out.println(String.format("%dms,Completed inserts: %d/%d", i * 100, completedInserts, options.getNumEvents()));
			}
			i++;
		}
		return completedInserts;
	}

	/**
	 * Execute the received producers, each on a separate thread.
	 * 
	 * @param producers
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static List<String> executeInThreads(ArrayList<Callable<List<String>>> producers) throws InterruptedException,
			ExecutionException {
		// Return of execution
		List<String> ids = new ArrayList<String>();

		ExecutorService executorService = Executors.newFixedThreadPool(producers.size());
		List<Future<List<String>>> futures = executorService.invokeAll(producers);

		// Wait for all producers to finish, and get the ids generated
		for (Future<List<String>> future : futures) {
			ids.addAll(future.get());
		}
		return ids;
	}

	/**
	 * Prepare producers that will send INSERT operations to the queue
	 * 
	 * @return
	 */
	private static ArrayList<Callable<List<String>>> prepareProducersForInsert() {
		return prepareProducers(OperationType.INSERT, null);
	}

	/**
	 * Prepare producers that will send SELECT operations to the queue for each id on ids list
	 * 
	 * @param ids
	 * @return
	 */
	private static ArrayList<Callable<List<String>>> prepareProducersForSelect(List<String> ids) {
		return prepareProducers(OperationType.SELECT, ids);
	}

	/**
	 * Prepare producers that will send INSERT or SELECT operations to the queue
	 * 
	 * @param operation
	 * @param ids
	 * @return
	 */
	private static ArrayList<Callable<List<String>>> prepareProducers(OperationType operation, List<String> ids) {

		ArrayList<Callable<List<String>>> producers = new ArrayList<Callable<List<String>>>();
		int requestPerProducer = options.getNumEvents() / producerWorkers;

		for (int i = 0; i < producerWorkers; i++) {
			Producer producer = cretateProducer(requestPerProducer, i);
			producer.setOperation(operation);
			if (operation == OperationType.INSERT) {
				producer.setJson(JSON);
			} else if (operation == OperationType.SELECT) {
				List<String> subList = ids.subList(i * requestPerProducer, (i + 1) * requestPerProducer);
				producer.setIdsToSelect(subList);
			}
			if (options.getQueueType() == QueueType.NONE) {
				((DirectProducer) producer).setDao(createDatabaseDAO());
			}
			producers.add(producer);
		}
		return producers;
	}

	/**
	 * Start defined number of consumers
	 * 
	 * @return
	 */
	private static ArrayList<Consumer> startConsumers() {

		ArrayList<Consumer> consumers = new ArrayList<Consumer>();
		for (int i = 0; i < options.getNumWorkers(); i++) {
			Consumer consumer = createConsumer(i);
			Thread thread = new Thread(consumer);
			thread.start();
			consumers.add(consumer);
		}
		// For ZeroMQ we need a Result consumer, in order to know when all the consumers have finished
		// http://zguide.zeromq.org/page:all#Divide-and-Conquer
		if (options.getQueueType() == QueueType.ZEROMQ) {
			resultsConsumer = new ZeroMQResultsConsumer(options.getNumEvents());
			Thread thread = new Thread(resultsConsumer);
			resultsConsumer.setDebug(options.debug());
			thread.start();
		}
		return consumers;
	}

	/**
	 * Create Database conection and cleans table/collection
	 * 
	 * @return
	 */
	private static DatabaseDAO connectAndCleanDB() {
		DatabaseDAO dao = createDatabaseDAO();
		dao.removeAll();
		return dao;
	}

	/**
	 * Remove all items inserted in the database and close all conectios
	 * 
	 * @param consumers
	 * @param dao
	 */
	private static void cleanAndcloseDBConnections(ArrayList<Consumer> consumers, DatabaseDAO dao) {
		try {
			// Clean database and close connections
			dao.removeAll();
			dao.close();
			for (Consumer consumer : consumers) {
				consumer.getDao().close();
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deletes the queue we were using for tests
	 */
	private static void deleteQueue() {
		try {
			Queue queue = getQueue();
			queue.deleteQueue();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * Blocking method, polling queue each 10ms until queue is empty
	 * 
	 * @throws InterruptedException
	 */
	private static void waitUntilEmptyQueue() throws InterruptedException {
		Queue queue = getQueue();
		int pendingMessages = queue.getPendingMessages();
		while (pendingMessages > 0) {
			Thread.sleep(10);
			pendingMessages = queue.getPendingMessages();
			if (options.debug()) {
				System.out.println("Pending messages: " + pendingMessages);
			}
		}
	}

}
