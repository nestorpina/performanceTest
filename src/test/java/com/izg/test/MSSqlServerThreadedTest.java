package com.izg.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.StopWatch;
import org.junit.AfterClass;
import org.junit.Assert;

import com.izg.test.tasks.MysqlReadTask;

/**
 * 
 * This tests inserts a json object in a ms sqlserver using the sqljdbc4 driver
 * After inserting the specified number of elements, using the number
 * of threads specified, it then retrieves each item by id
 *  
 * Configuration Steps :
 * <ul> 
 * <li>Download driver from : http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774</li> 
 * <li>Add to local repo : mvn install:install-file -Dfile=sqljdbc4.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0 -Dpackaging=jar</li>
 * <li>Add dependency :</li>
 * <pre> 
<dependency>
  <groupId>com.microsoft.sqlserver</groupId>
  <artifactId>sqljdbc4</artifactId>
  <version>4.0</version>
</dependency>
</pre>
 *
 * <li>Create table</li>
 * <pre>
sqlcmd -E -Q "CREATE TABLE testtable ( id VARCHAR(50)  NOT NULL, json TEXT NOT NULL);"
sqlcmd -E -Q "ALTER TABLE testtable ADD PRIMARY KEY (id)"
</pre>
 *
 * <li>sqlcmd commands :</li> 
<pre>
show databases : EXEC sp_databases
show tables : select name from master..sysobjects where xtype = 'U';
sqlcmd -E -Q "select count(*) from testtable;"
sqlcmd -E -Q "select count(*) from testtable;"
</pre>
 * </ul>
 * @author npina
 *
 */
public class MSSqlServerThreadedTest {

	private final static String jsonSmall = "{ 'LoadTime': 3, 'BackgroundImage': 'http://desprolamoderna.antena3.com/aplication/eIxlnfA8LPYl.png', 'DebugInfo': 1, 'RefreshInterval': 90, 'TimeTriggerMin': 150401, 'TimeTriggerMax': 150430, 'CodewordLatency': 10.08, 'ApplicationTitle': 'Prototipo Dev', 'facebookUrl': 'https:\\/\\/m.facebook.com', 'twitterUrl': 'https:\\/\\/m.twitter.com', 'Modules': [ { 'Id': 2, 'Name': 'Secciones', 'Programs': [ ] } , { 'Id': 3, 'Name': 'Participación', 'Contests': [ { 'Id': 20, 'Name': 'prueba concurso', 'Category': 'concurso', 'ImageIcon': 'http://desprolamoderna.antena3.com/contest/ZDH2Pg4P3F2t.png', 'ImageDetailed': 'http://desprolamoderna.antena3.com/contest/oezJ98JV1rLD.png', 'Description': 'oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu', 'Url': 'http:\\/\\/www.google.es', 'Questions': [ { 'Id': 44, 'Question': '¿Tu compañia de seguros reinvierte gran parte de sus beneficios en ti?', 'Time': 24, 'Background': 'http://desprolamoderna.antena3.com/pregunta/4YvyekkXnEcn.png', 'Answers': [ { 'Id': 110, 'Text': 'No No No', 'Correct': 1 } , { 'Id': 111, 'Text': 'Si', 'Correct': 0 } ] } , { 'Id': 45, 'Question': '¿Pregunta de prueba 2?', 'Time': 35, 'Background': 'http://desprolamoderna.antena3.com/pregunta/CbhQEURIpe9J.png', 'Answers': [ { 'Id': 112, 'Text': 'rpta uno', 'Correct': 0 } , { 'Id': 113, 'Text': 'rpta dos', 'Correct': 0 } , { 'Id': 114, 'Text': 'rpta 3', 'Correct': 1 } ] } , { 'Id': 46, 'Question': '¿Cargará la imagen de la campaña?', 'Time': 50, 'Answers': [ { 'Id': 115, 'Text': 'Sí', 'Correct': 0 } , { 'Id': 116, 'Text': 'No', 'Correct': 0 } , { 'Id': 117, 'Text': 'Tal vez', 'Correct': 0 } ] } ] } ] } , { 'Id': 1, 'Name': 'Guardado' } ] , 'Campaigns' : [ { 'Id': 272, 'Name': 'prueba pregunta', 'Codewords': [150441,150442], 'Background': 'http://desprolamoderna.antena3.com/imagen/9vrL8gMdTdHx.png', 'BackgroundMD5': '172c1847902aee8137be53a9bfbe458', 'Events': [ { 'Id': 566, 'Name': 'pregunta mutua', 'Description': 'Esta es la descripcion de facebook', 'QuestionId': 44, 'Start': 11.0, 'End': 20.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 567, 'Name': 'Lanza segunda pregunta', 'Description': 'pregunta dos', 'QuestionId': 45, 'Start': 25.0, 'End': 41.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 568, 'Name': 'Tercera pregunta', 'Description': 'pregunta 3', 'QuestionId': 46, 'Start': 45.0, 'End': 59.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 70.0, 'End': 170.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } , { 'Id': 37, 'Name': 'demoAtrapa1millonCorta', 'Codewords': [150435,150436], 'Background': 'http://agm28.blob.core.windows.net/imagen/eceNhoG1Acsk.png', 'Events': [ { 'Id': 63, 'Name': 'Eleccion Tema Seres', 'Start': 36.0, 'End': 50.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/sry7UoXYeqiF.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 64, 'Name': 'Resp1 ewoks', 'Start': 52.0, 'End': 54.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/9ph3YHqs2OFq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 65, 'Name': 'Resp 2 replicantes', 'Start': 56.0, 'End': 60.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/rYx8aspGR5b5.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 66, 'Name': 'Resp3 transformers', 'Start': 62.0, 'End': 64.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/eV9HD4xwh8Eq.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 67, 'Name': 'Resp4 avatares', 'Start': 65.0, 'End': 84.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/ppTong1SRzKd.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 74, 'Name': 'Pregunta1imagen', 'Start': 86.0, 'End': 90.0, 'EventType': 1, 'FileTag': 'http://agm28.blob.core.windows.net/content/Hd7XREgeuOTQ.png', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 69, 'Name': 'Pregunta1', 'QuestionId': 12, 'Start': 90.0, 'End': 224.0, 'EventType': 9, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': 113, 'Name': ' videoImagenio', 'Start': 1500.0, 'End': 2500.0, 'EventType': 2, 'FileTag': 'http://agm28.blob.core.windows.net/content/fxAZPIpy4VMC.mp4', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0, 'AllowsSharing': 1 } , { 'Id': -1, 'Name': 'Audio End', 'Start': 254.0, 'End': 354.0, 'EventType': 999, 'FileTag': '', 'TextTag': '', 'LikeButton': 0, 'AskButton': 0 } ] } ] }";

	@AfterClass
	public static void afterTest() throws InterruptedException {
	}


    private void testInsert(final int numberOfInserts,final int threadCount, final String json) throws InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
    	StopWatch stopwatch = new StopWatch();
    	stopwatch.start();

		final int insertsPerThread = numberOfInserts / threadCount;
		Callable<List<String>> insertTask = new Callable<List<String>>() {
			public List<String> call() {
				PreparedStatement preparedStatement = null;
				List<String> ids = new ArrayList<String>();
				try {
			    	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			    	final Connection conn = DriverManager.getConnection("jdbc:sqlserver://localhost;integratedSecurity=true");

					preparedStatement = conn.prepareStatement("insert into  testtable values (?, ?)");
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

					for (int i= 0; i < insertsPerThread; i++) {
						String id = UUID.randomUUID().toString();
						try {
							preparedStatement.setString(1, id);
							preparedStatement.setString(2, json);
							preparedStatement.execute();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						ids.add(id);
					}

				return ids;
			}
		};
        List<Callable<List<String>>> tasks = Collections.nCopies(threadCount, insertTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<List<String>>> futures = executorService.invokeAll(tasks);
        // Check for exceptions
        List<String> ids = new ArrayList<String>();
        for (Future<List<String>> future : futures) {
            // Throws an exception if an exception was thrown by the task.
        	ids.addAll(future.get());
        }
        stopwatch.suspend();
        System.out.println(String.format("Inserted %d (%d threads) in %s", ids.size(), threadCount, stopwatch));
        // Validate the number of json inserted
        Assert.assertEquals(ids.size(), insertsPerThread * threadCount);

        List<Callable<Integer>> readTasks = new ArrayList<Callable<Integer>>();
        for (int i = 0; i < threadCount; i++) {
        	List<String> subList = ids.subList(i*insertsPerThread, (i+1)*insertsPerThread);
        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    		Connection conn = DriverManager.getConnection("jdbc:sqlserver://localhost;integratedSecurity=true");

        	readTasks.add(new MysqlReadTask(conn, subList));

		}
        stopwatch.reset();
        stopwatch.start();

        List<Future<Integer>> readFutures = executorService.invokeAll(readTasks);

	    for (Future<Integer> future : readFutures) {
	    	Assert.assertEquals(Integer.valueOf(1),future.get());
		}
        stopwatch.suspend();
        System.out.println(String.format("Retrieved %d (%d threads) in %s", ids.size(), threadCount, stopwatch));

	}

//  @Test
  public void testInsert100000_10threads() throws InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
      testInsert(100000, 10, jsonSmall);
  }

//  @Test
  public void testInsert50000_10threads() throws InterruptedException, ExecutionException, ClassNotFoundException, SQLException {
      testInsert(50000,10, jsonSmall);
  }
}

