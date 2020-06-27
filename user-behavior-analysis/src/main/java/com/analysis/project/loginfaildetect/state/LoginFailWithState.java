package com.analysis.project.loginfaildetect.state;


import com.analysis.project.loginfaildetect.LoginEventParse;
import com.analysis.project.pojo.LoginEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LoginFailWithState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String path = LoginFailWithState.class.getClassLoader().getResource("LoginLog.csv")
            .getPath();
        SingleOutputStreamOperator<LoginEvent> dataSource = env.readTextFile(path)
            .map(new LoginEventParse()).filter(event -> null != event)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.milliseconds(3000)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getEventTime() * 1000;
                    }
                });

        SingleOutputStreamOperator<LoginEvent> result = dataSource
            .keyBy(loginEvent -> loginEvent.getUserId())
            .process(new FailLoginMatchFunction()); //统计两秒内失败的次数，有一次成功也认为是失败的
            //.process(new FailLoginMatchFunctionV2()); //只统计连续两次

        result.printToErr();

        env.execute("login fail detect job");
    }
}
