package com.analysis.project.loginfaildetect.state;


import com.analysis.project.pojo.LoginEvent;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FailLoginMatchFunction extends KeyedProcessFunction<Long, LoginEvent, LoginEvent> {

    private ListState<LoginEvent> loginEventListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loginEventListState = getRuntimeContext()
            .getListState(new ListStateDescriptor<LoginEvent>("login-fail-event", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginEvent> out)
        throws Exception {
        String eventType = value.getEventType();

        if ("fail".equals(eventType)) {
            loginEventListState.add(value);
        }

        ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000 + 2000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginEvent> out)
        throws Exception {
        ArrayList<LoginEvent> failEvents = Lists.newArrayList();
        if (null != loginEventListState.get()) {
            failEvents.addAll(Lists.newArrayList(loginEventListState.get()));
        }
        loginEventListState.clear();

        if (failEvents.size() > 1) {
            out.collect(failEvents.get(0));
        }
    }
}
