package com.analysis.project.loginfaildetect.state;


import com.analysis.project.pojo.LoginEvent;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FailLoginMatchFunctionV2 extends KeyedProcessFunction<Long, LoginEvent, LoginEvent> {

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
            if (null == loginEventListState.get()) {
                loginEventListState.update(Lists.newArrayList(value));
            } else {
                Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
                if (iterator.hasNext()) {
                    LoginEvent firstEvent = iterator.next();
                    if (value.getEventTime() < firstEvent.getEventTime() + 2) {
                        out.collect(value);
                    }
                    loginEventListState.update(Lists.newArrayList(value));
                } else {
                    loginEventListState.update(Lists.newArrayList(value));
                }
            }
        } else {
            loginEventListState.clear();
        }

    }

}
