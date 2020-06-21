package com.analysis.project.market;


import com.analysis.project.pojo.MarketingUserBehavior;
import com.google.common.collect.Lists;
import com.analysis.project.pojo.MarketingUserBehavior.MarketingUserBehaviorBuilder;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MarketingUserBehaviorSimulatedSource extends
    RichParallelSourceFunction<MarketingUserBehavior> {

    private boolean running = true;

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        int total = Integer.MAX_VALUE;

        ArrayList<String> channels = Lists
            .newArrayList("APP_STORE", "XIAOMI_STORE", "HUAWEI_STOPRE", "WEIBO", "TOUTIAO",
                "WECHAT");

        ArrayList<String> behaviors = Lists
            .newArrayList("INSTALL", "UNINSTALL", "BROWSE", "CLICK", "PURCHASE");

        Random random = new Random();
        int count = 0;
        while (running && count < total) {
            String channel = channels.get(random.nextInt(channels.size()));
            String behavior = behaviors.get(random.nextInt(behaviors.size()));

            MarketingUserBehaviorBuilder behaviorBuilder = MarketingUserBehavior.builder()
                .userId(UUID.randomUUID().toString())
                .behavior(behavior)
                .channel(channel)
                .timestamp(System.currentTimeMillis());

            ctx.collect(behaviorBuilder.build());

            Thread.sleep(10);
            count++;
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
