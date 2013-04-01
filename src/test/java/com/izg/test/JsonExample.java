package com.izg.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class JsonExample {
    public static void main(String argv[]) {
        try {
            JsonExample jsonExample = new JsonExample();
            jsonExample.testJackson();
        } catch (Exception e){
            System.out.println("Exception " + e);
        }
    }
    public void testJackson() throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        String from = "{ \"LoadTime\": 3, \"BackgroundImage\": \"http://desprolamoderna.antena3.com/aplication/eIxlnfA8LPYl.png\", \"DebugInfo\": 1, \"RefreshInterval\": 90, \"TimeTriggerMin\": 150401, \"TimeTriggerMax\": 150430, \"CodewordLatency\": 10.08, \"ApplicationTitle\": \"Prototipo Dev\", \"facebookUrl\": \"https:\\/\\/m.facebook.com\", \"twitterUrl\": \"https:\\/\\/m.twitter.com\", \"Modules\": [ { \"Id\": 2, \"Name\": \"Secciones\", \"Programs\": [ ] } , { \"Id\": 3, \"Name\": \"Participación\", \"Contests\": [ { \"Id\": 20, \"Name\": \"prueba concurso\", \"Category\": \"concurso\", \"ImageIcon\": \"http://desprolamoderna.antena3.com/contest/ZDH2Pg4P3F2t.png\", \"ImageDetailed\": \"http://desprolamoderna.antena3.com/contest/oezJ98JV1rLD.png\", \"Description\": \"oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu oiuygiu yfguitfufguyg uigi ghu\", \"Url\": \"http:\\/\\/www.google.es\", \"Questions\": [ { \"Id\": 44, \"Question\": \"¿Tu compañia de seguros reinvierte gran parte de sus beneficios en ti?\", \"Time\": 24, \"Background\": \"http://desprolamoderna.antena3.com/pregunta/4YvyekkXnEcn.png\", \"Answers\": [ { \"Id\": 110, \"Text\": \"No No No\", \"Correct\": 1 } , { \"Id\": 111, \"Text\": \"Si\", \"Correct\": 0 } ] } , { \"Id\": 45, \"Question\": \"¿Pregunta de prueba 2?\", \"Time\": 35, \"Background\": \"http://desprolamoderna.antena3.com/pregunta/CbhQEURIpe9J.png\", \"Answers\": [ { \"Id\": 112, \"Text\": \"rpta uno\", \"Correct\": 0 } , { \"Id\": 113, \"Text\": \"rpta dos\", \"Correct\": 0 } , { \"Id\": 114, \"Text\": \"rpta 3\", \"Correct\": 1 } ] } , { \"Id\": 46, \"Question\": \"¿Cargará la imagen de la campaña?\", \"Time\": 50, \"Answers\": [ { \"Id\": 115, \"Text\": \"Sí\", \"Correct\": 0 } , { \"Id\": 116, \"Text\": \"No\", \"Correct\": 0 } , { \"Id\": 117, \"Text\": \"Tal vez\", \"Correct\": 0 } ] } ] } ] } , { \"Id\": 1, \"Name\": \"Guardado\" } ] , \"Campaigns\" : [ { \"Id\": 272, \"Name\": \"prueba pregunta\", \"Codewords\": [150441,150442], \"Background\": \"http://desprolamoderna.antena3.com/imagen/9vrL8gMdTdHx.png\", \"BackgroundMD5\": \"172c1847902aee8137be53a9bfbe458\", \"Events\": [ { \"Id\": 566, \"Name\": \"pregunta mutua\", \"Description\": \"Esta es la descripcion de facebook\", \"QuestionId\": 44, \"Start\": 11.0, \"End\": 20.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 567, \"Name\": \"Lanza segunda pregunta\", \"Description\": \"pregunta dos\", \"QuestionId\": 45, \"Start\": 25.0, \"End\": 41.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 568, \"Name\": \"Tercera pregunta\", \"Description\": \"pregunta 3\", \"QuestionId\": 46, \"Start\": 45.0, \"End\": 59.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": -1, \"Name\": \"Audio End\", \"Start\": 70.0, \"End\": 170.0, \"EventType\": 999, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0 } ] } , { \"Id\": 37, \"Name\": \"demoAtrapa1millonCorta\", \"Codewords\": [150435,150436], \"Background\": \"http://agm28.blob.core.windows.net/imagen/eceNhoG1Acsk.png\", \"Events\": [ { \"Id\": 63, \"Name\": \"Eleccion Tema Seres\", \"Start\": 36.0, \"End\": 50.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/sry7UoXYeqiF.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 64, \"Name\": \"Resp1 ewoks\", \"Start\": 52.0, \"End\": 54.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/9ph3YHqs2OFq.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 65, \"Name\": \"Resp 2 replicantes\", \"Start\": 56.0, \"End\": 60.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/rYx8aspGR5b5.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 66, \"Name\": \"Resp3 transformers\", \"Start\": 62.0, \"End\": 64.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/eV9HD4xwh8Eq.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 67, \"Name\": \"Resp4 avatares\", \"Start\": 65.0, \"End\": 84.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/ppTong1SRzKd.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 74, \"Name\": \"Pregunta1imagen\", \"Start\": 86.0, \"End\": 90.0, \"EventType\": 1, \"FileTag\": \"http://agm28.blob.core.windows.net/content/Hd7XREgeuOTQ.png\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 69, \"Name\": \"Pregunta1\", \"QuestionId\": 12, \"Start\": 90.0, \"End\": 224.0, \"EventType\": 9, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": 113, \"Name\": \" videoImagenio\", \"Start\": 1500.0, \"End\": 2500.0, \"EventType\": 2, \"FileTag\": \"http://agm28.blob.core.windows.net/content/fxAZPIpy4VMC.mp4\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0, \"AllowsSharing\": 1 } , { \"Id\": -1, \"Name\": \"Audio End\", \"Start\": 254.0, \"End\": 354.0, \"EventType\": 999, \"FileTag\": \"\", \"TextTag\": \"\", \"LikeButton\": 0, \"AskButton\": 0 } ] } ] }";
        TypeReference<HashMap<String,Object>> typeRef
              = new TypeReference<
                     HashMap<String,Object>
                   >() {};
        HashMap<String,Object> o
             = mapper.readValue(from, typeRef);
        System.out.println("Got " + o);
        Map<String, String> flattenHashMap = flattenHashMap(o,"");
		System.out.println(flattenHashMap);
		System.out.print("{");
        for (String key : flattenHashMap.keySet()) {
        	System.out.print(String.format("\"%s\":\"%s\",", key, flattenHashMap.get(key)));
		}
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private Map<String,String> flattenHashMap(Map<String, Object> map, String prefix) {
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
	private Map<String,String> flattenList(List<Object> list, String prefix) {
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