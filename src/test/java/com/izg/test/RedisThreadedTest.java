package com.izg.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.StopWatch;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import com.izg.test.tasks.RedisReadTask;

public class RedisThreadedTest {

	private final static String jsonSmall = "{ \"LoadTime\": 3, \"BackgroundImage\": \"http://desprolamoderna.antena3.com/aplication/eIxlnfA8LPYl.png\", \"DebugInfo\": 1, \"RefreshInterval\": 90, \"TimeTriggerMin\": 150401, \"TimeTriggerMax\": 150430, \"CodewordLatency\": 10.08, \"ApplicationTitle\": \"Prototipo Dev\", \"facebookUrl\": \"https:\\/\\/m.facebook.com\", \"twitterUrl\": \"https:\\/\\/m.twitter.com\", \"Modules\": [ { \"Id\": 2, \"Name\": \"Secciones\", \"Programs\": [ ] } , { \"Id\": 3, \"Name\": \"Participación\", \"Contests\": [ { \"Id\": 20, \"Name\": \"prueba concurso\", \"Category\": \"concurso\", \"ImageIcon\": \"http://desprolamoderna.antena3.com/contest/ZDH2Pg4P3F2t.png\", \"ImageDetailed\": \"http://desprolamoderna.antena3.com/contest/oezJ98JV1rLD.png\", \"Description\": \"oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu\", \"Url\": \"http:\\/\\/www.google.es\", \"Questions\": [ { \"Id\": 44, \"Question\": \"¿Tu compañia de seguros reinvierte gran parte de sus beneficios en ti?\", \"Time\": 24, \"Background\": \"http://desprolamoderna.antena3.com/pregunta/4YvyekkXnEcn.png\", \"Answers\": [ { \"Id\": 110, \"Text\": \"No No No\", \"Correct\": 1 } , { \"Id\": 111, \"Text\": \"Si\", \"Correct\": 0 } ] } , { \"Id\": 45, \"Question\": \"¿Pregunta de prueba 2?\", \"Time\": 35, \"Background\": \"http://desprolamoderna.antena3.com/pregunta/CbhQEURIpe9J.png\", \"Answers\": [ { \"Id\": 112, \"Text\": \"rpta uno\", \"Correct\": 0 } , { \"Id\": 113, \"Text\": \"rpta dos\", \"Correct\": 0 } , { \"Id\": 114, \"Text\": \"rpta 3\", \"Correct\": 1 } ] } , { \"Id\": 46, \"Question\": \"¿Cargará la imagen de la campaña?\", \"Time\": 50, \"Answers\": [ { \"Id\": 115, \"Text\": \"Sí\", \"Correct\": 0 } , { \"Id\": 116, \"Text\": \"No\", \"Correct\": 0 } , { \"Id\": 117, \"Text\": \"Tal vez\", \"Correct\": 0 } ] } ] } ] } , { \"Id\": 1, \"Name\": \"Guardado\" } ] , \"Campaigns\" : [ { \"Id\": 272, \"Name\": \"prueba pregunta\", \"Codewords\": [150441,150442], \"Background\": \"http://desprolamoderna.antena3.com/imagen/9vrL8gMdTdHx.png\", \"BackgroundMD5\": \"172c1847902aee8137be53a9bfbe458\", \"Events\": [ { \"Id\": 566, \"Name\": \"pregunta mutua\", \"Description\": \"Esta es la descripcion de facebook\", \"QuestionId\": 44, \"Start\": 11.0, \"End\": 20.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 567, \"Name\": \"Lanza segunda pregunta\", \"Description\": \"pregunta dos\", \"QuestionId\": 45, \"Start\": 25.0, \"End\": 41.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 568, \"Name\": \"Tercera pregunta\", \"Description\": \"pregunta 3\", \"QuestionId\": 46, \"Start\": 45.0, \"End\": 59.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": -1, \"Name\": \"Audio End\", \"Start\": 70.0, \"End\": 170.0, \"EventType\": 999, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0 } ] } , { \"Id\": 37, \"Name\": \"demoAtrapa1millonCorta\", \"Codewords\": [150435,150436], \"Background\": \"http://agm28.blob.core.windows.net/imagen/eceNhoG1Acsk.png\", \"Events\": [ { \"Id\": 63, \"Name\": \"Eleccion Tema Seres\", \"Start\": 36.0, \"End\": 50.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/sry7UoXYeqiF.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 64, \"Name\": \"Resp1 ewoks\", \"Start\": 52.0, \"End\": 54.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/9ph3YHqs2OFq.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 65, \"Name\": \"Resp 2 replicantes\", \"Start\": 56.0, \"End\": 60.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/rYx8aspGR5b5.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 66, \"Name\": \"Resp3 transformers\", \"Start\": 62.0, \"End\": 64.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/eV9HD4xwh8Eq.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 67, \"Name\": \"Resp4 avatares\", \"Start\": 65.0, \"End\": 84.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/ppTong1SRzKd.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 74, \"Name\": \"Pregunta1imagen\", \"Start\": 86.0, \"End\": 90.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/Hd7XREgeuOTQ.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 69, \"Name\": \"Pregunta1\", \"QuestionId\": 12, \"Start\": 90.0, \"End\": 224.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 113, \"Name\": \" videoImagenio\", \"Start\": 1500.0, \"End\": 2500.0, \"EventType\": 2, \"FileTag\": \"http://agm28.blob.core.windows.net/content/fxAZPIpy4VMC.mp4\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": -1, \"Name\": \"Audio End\", \"Start\": 254.0, \"End\": 354.0, \"EventType\": 999, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0 } ] } ] }";

	private static Map<String, String> flattenHashMap = null;

	{
		try {
			StopWatch s = new StopWatch();
			s.start();
			flattenHashMap = flattenHashMap(parseJson(jsonSmall),"");
			s.suspend();
			System.out.println("Time to parse and flatten json:"+s);

		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


    private void testInsertAndRead(final int numberOfInserts,final int threadCount, final String json) throws InterruptedException, ExecutionException {
    	StopWatch stopwatch = new StopWatch();
    	stopwatch.start();

 		final int insertsPerThread = numberOfInserts / threadCount;


        Callable<List<String>> task = new Callable<List<String>>() {
            public List<String> call() {
        		Jedis jedis = new Jedis("localhost", Protocol.DEFAULT_PORT);
        		jedis.connect();

            	long i=0;
            	List<String> ids = new ArrayList<String>();
            	for(;i<insertsPerThread;i++) {
					String id = UUID.randomUUID().toString();
//					jedis.set(id, json);
					jedis.hmset(id, flattenHashMap);
					ids.add(id);
            	}
            	jedis.disconnect();
    			return ids;
            }
        };
        List<Callable<List<String>>> tasks = Collections.nCopies(threadCount, task);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<List<String>>> futures = executorService.invokeAll(tasks);
        // Check for exceptions
        List<String> idsInserted = new ArrayList<String>();
        for (Future<List<String>> future : futures) {
            // Throws an exception if an exception was thrown by the task.
        	idsInserted.addAll(future.get());
        }
        stopwatch.suspend();
        System.out.println(String.format("Inserted %d (%d threads) in %s", numberOfInserts, threadCount, stopwatch));

        // Validate the number of json inserted
        Assert.assertEquals(insertsPerThread * threadCount, idsInserted.size());

        List<Callable<Integer>> readTasks = new ArrayList<Callable<Integer>>();
        for (int i = 0; i < threadCount; i++) {
        	List<String> subList = idsInserted.subList(i*insertsPerThread, (i+1)*insertsPerThread);
        	readTasks.add(new RedisReadTask(subList, flattenHashMap.keySet()));

		}
        stopwatch.reset();
        stopwatch.start();

        List<Future<Integer>> readFutures = executorService.invokeAll(readTasks);

	    for (Future<Integer> future : readFutures) {
	    	Assert.assertEquals(Integer.valueOf(1),future.get());
		}
        stopwatch.suspend();
        System.out.println(String.format("Retrieved %d (%d threads) in %s", numberOfInserts, threadCount, stopwatch));


	}

	@Test
	public void testInsert100000_10threads() throws InterruptedException,
			ExecutionException {
		testInsertAndRead(100000, 10, jsonSmall);
	}

//	@Test
	public void testIsert50000_10threads() throws InterruptedException,
			ExecutionException {
		testInsertAndRead(50000, 10, jsonSmall);
	}

    private static HashMap<String, Object> parseJson(String json) throws JsonParseException, JsonMappingException, IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        TypeReference<HashMap<String,Object>> typeRef
              = new TypeReference<
                     HashMap<String,Object>
                   >() {};
        HashMap<String,Object> o
             = mapper.readValue(json, typeRef);
        return o;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map<String,String> flattenHashMap(Map<String, Object> map, String prefix) {
    	HashMap<String,String> result = new HashMap<String, String>();
    	Set<String> keys = map.keySet();
    	for (String key : keys) {
			Object object = map.get(key);
			if (object instanceof Map) {
				result.putAll(flattenHashMap((Map<String, Object>)object, prefix+key+"_"));
			} else if (object instanceof List) {
				result.putAll(flattenList((List)object, prefix+key+"_"));
			} else {
				result.put(prefix + key, object.toString());
			}
		}
    	return result;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map<String,String> flattenList(List<Object> list, String prefix) {
    	HashMap<String,String> result = new HashMap<String, String>();
		for (int i=0; i < list.size(); i++) {
			Object object = list.get(i);
			String key = prefix+"["+i+"]";
			if (object instanceof Map) {
				result.putAll(flattenHashMap((Map<String, Object>)object, key+"_"));
			} else if (object instanceof List) {
				result.putAll(flattenList((List)object, key+"_"));
			} else {
				result.put(key, object.toString());
			}

		}
		return result;
    }

}

