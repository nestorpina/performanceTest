package com.izg.test.tasks;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;

import com.google.gson.Gson;
import com.izg.test.DatastoreRemoteThreadedTest;
import com.izg.test.json.JsonResponse;

public class DatastoreRemoteReadTask implements Callable<Integer> {
	private static final String URL_SELECT = "http://"+DatastoreRemoteThreadedTest.URL_HOST+"/s/performance/select/";
	private final List<String> list;

	public DatastoreRemoteReadTask(List<String> list) {

		this.list = list;
	}

	public Integer call() throws Exception {

		long time = 0;
		HttpClient httpclient = new DefaultHttpClient();
		for (String id : list) {
			JsonResponse json = null;
	         try {
	        	 HttpGet httpget = new HttpGet(URL_SELECT+id);

	             ResponseHandler<String> responseHandler = new BasicResponseHandler();
	             String responseBody = httpclient.execute(httpget, responseHandler);
//	             System.out.println(responseBody);
	             json = new Gson().fromJson(responseBody, JsonResponse.class);
	             time += json.getTime();
	         } catch (ClientProtocolException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
	             httpclient.getConnectionManager().shutdown();
	         }

			Assert.assertNotNull(json);
		}
		return Integer.valueOf(""+time);
	}
}