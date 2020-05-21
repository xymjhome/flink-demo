package streamapi.util;


import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class JsonToMapUtil {

    public static final Gson GSON = new Gson();

    public static Map<String, Object>  parseJsonToMap(String json) {
        if (StringUtils.isBlank(json)) {
            return Maps.newHashMap();
        }
        Map<String, Object> jsonMap = GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());

        return parseToMap("", jsonMap);
    }

    private static Map<String, Object> parseToMap(String key, Map<String, Object> jsonMap) {
        HashMap<String, Object> resultMap = Maps.newHashMap();
        for (String innerKey : jsonMap.keySet()) {
            if (StringUtils.isNotBlank(key)) {
                resultMap.put(key + "." + innerKey, jsonMap.get(innerKey));
            } else {
                resultMap.put(innerKey, jsonMap.get(innerKey));
            }

        }

        Map<String, Object> map = Maps.newHashMap();
        for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
            Object valueObj = entry.getValue();
            if (valueObj instanceof Map) {
                Map<String, Object> innerMap = parseToMap(entry.getKey(),
                    (Map<String, Object>) valueObj);
                if (MapUtils.isNotEmpty(innerMap)) {
                    map.putAll(innerMap);
                }
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;

    }

}
