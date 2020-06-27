package com.analysis.project.loginfaildetect.cep;


import akka.japi.tuple.Tuple3;
import com.analysis.project.pojo.LoginEvent;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cep.PatternSelectFunction;

public class FailLoginPatternSelectFunction implements PatternSelectFunction<LoginEvent, Tuple3<Long, String, String>> {

    @Override
    public Tuple3<Long, String, String> select(Map<String, List<LoginEvent>> pattern) throws Exception {
        List<LoginEvent> begin = pattern.getOrDefault("begin", Lists.newArrayList());
        List<LoginEvent> next = pattern.getOrDefault("next", Lists.newArrayList());
        if (CollectionUtils.isEmpty(next)) {
            return new Tuple3(111l, "next", "miss");
        }
        return new Tuple3<>(next.get(0).getUserId(), String.valueOf(next.get(0).getEventTime()), next.get(0).getEventType());
    }
}
