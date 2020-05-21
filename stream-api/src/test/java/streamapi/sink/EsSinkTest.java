package streamapi.sink;

import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import scala.tools.nsc.doc.base.comment.Bold;
import streamapi.pojo.Sensor;

@Slf4j
public class EsSinkTest {

    private static final Gson GSON = new Gson();

    @Test
    public void test() {
        Sensor sensor = Sensor.builder().id("111").timestamp(11111l).temperature(20.78).build();
        String json = GSON.toJson(sensor);
        log.error("save data:" + json);
        Map<String, Object> map = GSON.fromJson(json, Map.class);
        log.error(map.toString());

        String string = "{\"public_response\":{\"msg\":\"success\",\"result\":{\"order_state\":true},\"error\":1}}";
        Map<String, Object> ts = GSON.fromJson(string, Map.class);
        log.error(ts.toString());

        Map<String, Object> stringObjectMap = parseToMap("", ts);
        log.error(stringObjectMap.toString());
    }

    public Map<String, Object> parseToMap(String key, Map<String, Object> ts) {
        HashMap<String, Object> resultMap = Maps.newHashMap();
        for (String innerKey : ts.keySet()) {
            if (StringUtils.isNotBlank(key)) {
                resultMap.put(key + "." + innerKey, ts.get(innerKey));
            } else {
                resultMap.put(innerKey, ts.get(innerKey));
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
