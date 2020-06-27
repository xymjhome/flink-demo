package com.analysis.project.loginfaildetect.cep;


import akka.japi.tuple.Tuple3;
import com.analysis.project.loginfaildetect.LoginEventParse;
import com.analysis.project.loginfaildetect.state.LoginFailWithState;
import com.analysis.project.pojo.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LoginFailWithCep {

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

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent value) throws Exception {
                    return "fail".equals(value.getEventType());
                }
            }).next("next")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent value) throws Exception {
                    return "fail".equals(value.getEventType());
                }
            }).within(Time.seconds(2));

        PatternStream<LoginEvent> patternStream = CEP
            .pattern(dataSource.keyBy(event -> event.getUserId()), pattern);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> result = patternStream
            .select(new FailLoginPatternSelectFunction());

        result.printToErr();

        env.execute("login fail detect cep");
    }
}
