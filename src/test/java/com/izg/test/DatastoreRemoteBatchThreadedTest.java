package com.izg.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.StopWatch;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;

import com.google.gson.Gson;
import com.googlecode.objectify.ObjectifyService;
import com.igz.performance.database.datastore.DatastoreObject;
import com.izg.test.json.JsonResponse2;

/**
 *
 * @author npina
 * This tests makes http requests to nestor-shop.appspot.com/s/performance/insert/{number} , which does
 * {number} of inserts in the datastore. After inserting the specified number of elements, using the number
 * of threads specified, it thens retrieves each item by calling nestor-shop.appspot.com/s/performance/select/{id}

 */
public class DatastoreRemoteBatchThreadedTest {


//    public static final String URL_HOST = "localhost:8080";
	public static final String URL_HOST = "nestor-shop.appspot.com";
	private static final String URL_INSERT = "http://"+URL_HOST+"/s/performance/insertselect";

	@AfterClass
	public static void afterTest() throws InterruptedException {
	}


    private void testInsert(final int numberOfInserts,final int threadCount) throws InterruptedException, ExecutionException, ClientProtocolException, IOException {
    	StopWatch stopwatch = new StopWatch();
    	stopwatch.start();

		final int insertsPerThread = numberOfInserts / threadCount;
		Callable<List<JsonResponse2>> insertTask = new Callable<List<JsonResponse2>>() {
			public List<JsonResponse2> call() {
				ObjectifyService.register(DatastoreObject.class);
				List<JsonResponse2> responses = new ArrayList<JsonResponse2>();

		    	 HttpClient httpclient = new DefaultHttpClient();
		    	 JsonResponse2 json = null;
		         try {
		        	 HttpGet httpget = new HttpGet(URL_INSERT+"/"+insertsPerThread);

		             ResponseHandler<String> responseHandler = new BasicResponseHandler();
		             String responseBody = httpclient.execute(httpget, responseHandler);
//			             System.out.println(responseBody);
		             json = new Gson().fromJson(responseBody, JsonResponse2.class);
		             

		         } catch (ClientProtocolException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
		             httpclient.getConnectionManager().shutdown();
		         }
				responses.add(json);

				return responses;
			}
		};
        List<Callable<List<JsonResponse2>>> tasks = Collections.nCopies(threadCount, insertTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<List<JsonResponse2>>> futures = executorService.invokeAll(tasks);
        // Check for exceptions
        List<JsonResponse2> responses = new ArrayList<JsonResponse2>();
        for (Future<List<JsonResponse2>> future : futures) {
            // Throws an exception if an exception was thrown by the task.
        	responses.addAll(future.get());
        }
        stopwatch.suspend();
        long timeInserts = 0;
        long timeSelects = 0;
        for (JsonResponse2 response : responses) {
			timeInserts += response.getInserttime();
			timeSelects += response.getSelecttime();
		}
        System.out.println(String.format("Inserted and selected %d (%d threads) in %dms (insert time: %dms, select time: %dms)", responses.size()*insertsPerThread, threadCount, stopwatch.getTime(), timeInserts, timeSelects));

	}

//  @Test
  public void testInsert100000_10threads() throws InterruptedException, ExecutionException, ClientProtocolException, IOException{
      testInsert(1,1);
  }

//  @Test
  public void testInsert50000_10threads() throws InterruptedException, ExecutionException, ClientProtocolException, IOException {
      testInsert(5000,10);
  }
}

