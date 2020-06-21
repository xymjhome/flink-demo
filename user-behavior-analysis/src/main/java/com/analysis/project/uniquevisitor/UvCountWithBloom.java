package com.analysis.project.uniquevisitor;


import com.analysis.project.pojo.UserUVCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

@Slf4j
public class UvCountWithBloom extends
    ProcessWindowFunction<Tuple2<String, Long>, UserUVCount, String, TimeWindow> {

    private static final Jedis jedis = new Jedis("127.0.0.1", 6379);
    private static final SimpleBloomHash bloomHash = new SimpleBloomHash(1 << 29);

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements,
        Collector<UserUVCount> out) throws Exception {

        String key = String.valueOf(context.window().getEnd());
        //log.error("key :" + key);
        String currentKeyCount = jedis.hget("all_count", key);
        Integer count = 0;
        if (StringUtils.isNumeric(currentKeyCount)) {
            count = Integer.parseInt(currentKeyCount);
        }

        Tuple2<String, Long> tuple2 = elements.iterator().next();
        if (null != tuple2) {
            Long offset = bloomHash.hash(String.valueOf(tuple2.f1), 61);
            Boolean getbit = jedis.getbit(key, offset);
            if (getbit) {
//                out.collect(UserUVCount.builder()
//                    .windowEnd(context.window().getEnd())
//                    .count(count).build());
            } else {
                jedis.setbit(key, offset, true);
                jedis.hset("all_count", key, String.valueOf(count + 1));
//                out.collect(UserUVCount.builder()
//                    .windowEnd(context.window().getEnd())
//                    .count(count + 1).build());
            }
        }

    }
}
