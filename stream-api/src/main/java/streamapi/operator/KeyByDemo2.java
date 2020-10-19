package streamapi.operator;


import com.google.common.collect.Lists;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class KeyByDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<Entry> streamSource = environment
            .fromCollection(Entry.generateEntrys()).setParallelism(1);

        //testKeyBy(streamSource);
        streamSource.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                //System.out.println("numPartitions:" + numPartitions);
                int partition = (int)Math.floor((key % 16) * numPartitions / 16);
                System.out.println("partition:" + partition + "--->" + "key id:" + key);
                return partition;
            }
        }, Entry::getId).flatMap(new RichFlatMapFunction<Entry, String>() {
            private ListState<Entry> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                listState = getRuntimeContext()
//                    .getListState(new ListStateDescriptor<Entry>("list_state", Entry.class));
            }

            @Override
            public void flatMap(Entry value, Collector<String> out) throws Exception {
                //System.out.println("-------------Group start-------------------");
                //System.out.println("id:" + value.id);
                int partition = (int)Math.floor((value.id % 16) * 6 / 16);
                out.collect(partition + "<-->" + String.valueOf(value.id));
                //System.out.println("-------------Group end-------------------");
            }
        }).setParallelism(6).printToErr().setParallelism(6);

        System.out.println("end:" + environment.getExecutionPlan());
        environment.execute("keyBy demo");
    }

    private static void testKeyBy(DataStreamSource<Entry> streamSource) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        streamSource.keyBy(Entry::getId)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
            .apply(new WindowFunction<Entry, String, Integer, TimeWindow>() {
                @Override
                public void apply(Integer key, TimeWindow window, Iterable<Entry> input,
                    Collector<String> out) throws Exception {
                    List list = IteratorUtils.toList(input.iterator());
                    long start = window.getStart();
                    System.out
                        .println("**** start time:" + dateFormat.format(new Date(start)) + " ****");
                    System.out
                        .println("key:" + key + "  entry size:" + list.size() + " entry:" + list);
                    System.out.println(
                        "**** end time:" + dateFormat.format(new Date(window.getEnd())) + " ****");
                    out.collect("window size:" + list.size());
                }
            }).setParallelism(5).printToErr();
    }


    @Data
    public static class Entry {

        private int id;
        private String name;


        public static List<Entry> generateEntrys() {
            Random random = new Random();
            int count = 1000;
            ArrayList<Entry> arrayList = Lists.newArrayList();
            while (count > 0) {
                int nextInt = random.nextInt(100000000);
                Entry entry = new Entry();
                entry.id = nextInt;
                entry.name = "name_" + count + "_" + nextInt;
                arrayList.add(entry);
//                Entry entry2 = new Entry();
//                entry2.id = nextInt;
//                entry2.name = "name2_" + count + "_" + nextInt;
//                arrayList.add(entry2);
                count--;
            }

            return arrayList;
        }
    }


}
