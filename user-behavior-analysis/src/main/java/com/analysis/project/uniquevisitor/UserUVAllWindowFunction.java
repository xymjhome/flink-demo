package com.analysis.project.uniquevisitor;


import com.analysis.project.pojo.UserBehavior;
import com.analysis.project.pojo.UserUVCount;
import com.google.common.collect.Lists;
import com.analysis.project.pojo.UserUVCount.UserUVCountBuilder;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserUVAllWindowFunction implements
    AllWindowFunction<UserBehavior, UserUVCount, TimeWindow> {


    @Override
    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UserUVCount> out)
        throws Exception {
        List<Long> userIds = Lists.newArrayList(values).stream().map(UserBehavior::getUserId)
            .distinct().collect(Collectors.toList());

        UserUVCountBuilder uvCountBuilder = UserUVCount.builder().count(userIds.size())
            .windowEnd(window.getEnd());

        out.collect(uvCountBuilder.build());
    }
}
