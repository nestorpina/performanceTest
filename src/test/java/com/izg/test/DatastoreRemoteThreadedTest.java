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
import org.junit.Assert;

import com.google.gson.Gson;
import com.googlecode.objectify.ObjectifyService;
import com.igz.performance.database.datastore.DatastoreObject;
import com.izg.test.json.JsonResponse;
import com.izg.test.tasks.DatastoreRemoteReadTask;

/**
 *
 * @author npina
 *
 * This tests makes http requests to nestor-shop.appspot.com/s/performance/insert , which inserts
 * a json item in the datastore. After inserting the specified number of elements, using the number
 * of threads specified, it thens retrieves each item by calling nestor-shop.appspot.com/s/performance/select/{id}
 */
public class DatastoreRemoteThreadedTest {


//    public static final String URL_HOST = "localhost:8080";
	public static final String URL_HOST = "nestor-shop.appspot.com";
	private static final String URL_INSERT = "http://"+URL_HOST+"/s/performance/insert";

	@AfterClass
	public static void afterTest() throws InterruptedException {
	}


    private void testInsert(final int numberOfInserts,final int threadCount) throws InterruptedException, ExecutionException, ClientProtocolException, IOException {
    	StopWatch stopwatch = new StopWatch();
    	stopwatch.start();

		final int insertsPerThread = numberOfInserts / threadCount;
		Callable<List<JsonResponse>> insertTask = new Callable<List<JsonResponse>>() {
			public List<JsonResponse> call() {
				ObjectifyService.register(DatastoreObject.class);
				List<JsonResponse> responses = new ArrayList<JsonResponse>();

				for (int i= 0; i < insertsPerThread; i++) {
			    	 HttpClient httpclient = new DefaultHttpClient();
			    	 JsonResponse json = null;
			         try {
			        	 HttpGet httpget = new HttpGet(URL_INSERT);

			             ResponseHandler<String> responseHandler = new BasicResponseHandler();
			             String responseBody = httpclient.execute(httpget, responseHandler);
//			             System.out.println(responseBody);
			             json = new Gson().fromJson(responseBody, JsonResponse.class);
			             

			         } catch (ClientProtocolException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
			             httpclient.getConnectionManager().shutdown();
			         }
					responses.add(json);
				}

				return responses;
			}
		};
        List<Callable<List<JsonResponse>>> tasks = Collections.nCopies(threadCount, insertTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<List<JsonResponse>>> futures = executorService.invokeAll(tasks);
        // Check for exceptions
        List<JsonResponse> responses = new ArrayList<JsonResponse>();
        List<String> ids = new ArrayList<String>();
        for (Future<List<JsonResponse>> future : futures) {
            // Throws an exception if an exception was thrown by the task.
        	responses.addAll(future.get());
        }
        stopwatch.suspend();
        long timeInserts = 0;
        for (JsonResponse response : responses) {
			ids.add(response.getId());
			timeInserts += response.getTime();
		}
        System.out.println(String.format("Inserted %d (%d threads) in %dms (without http time: %dms)", ids.size(), threadCount, stopwatch.getTime(), timeInserts));
        // Validate the number of json inserted
        Assert.assertEquals(ids.size(), insertsPerThread * threadCount);

        List<Callable<Integer>> readTasks = new ArrayList<Callable<Integer>>();
        for (int i = 0; i < threadCount; i++) {
        	List<String> subList = ids.subList(i*insertsPerThread, (i+1)*insertsPerThread);

        	readTasks.add(new DatastoreRemoteReadTask(subList));

		}
        stopwatch.reset();
        stopwatch.start();

        List<Future<Integer>> readFutures = executorService.invokeAll(readTasks);

        long timeSelects = 0;
	    for (Future<Integer> future : readFutures) {
	    	Integer time = future.get();
	    	// A result of 0 indicates that we didn't retrieve results
			Assert.assertNotEquals(Integer.valueOf(0),time);
			timeSelects += time;
		}
        stopwatch.suspend();
        System.out.println(String.format("Retrieved %d (%d threads) in %dms (without http time: %dms)", ids.size(), threadCount, stopwatch.getTime(), timeSelects));

	}

//  @Test
  public void testInsert100000_10threads() throws InterruptedException, ExecutionException, ClientProtocolException, IOException{
      testInsert(10, 10);
  }

//  @Test
  public void testInsert50000_10threads() throws InterruptedException, ExecutionException, ClientProtocolException, IOException {
      testInsert(50000,10);
  }
}

