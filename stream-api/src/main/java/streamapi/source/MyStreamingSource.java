package streamapi.source;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import streamapi.pojo.DataItem;

public class MyStreamingSource implements SourceFunction<DataItem> {

    private boolean isRun = true;

    @Override
    public void run(SourceContext<DataItem> ctx) throws Exception {
        while (isRun) {
            int id = new Random().nextInt(100);
            ArrayList<String> names = Lists.newArrayList("flink", "spark", "storm");
            ctx.collect(new DataItem(id, names.get(new Random().nextInt(3))));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }
}
