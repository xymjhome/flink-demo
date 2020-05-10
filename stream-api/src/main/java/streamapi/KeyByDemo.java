package streamapi;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        ArrayList<Tuple3<Integer, Integer, Integer>> tuple3s = Lists
            .newArrayList(new Tuple3<>(0, 1, 0),
                new Tuple3<>(0, 1, 1),
                new Tuple3<>(0, 2, 2),
                new Tuple3<>(0, 1, 3),
                new Tuple3<>(1, 2, 5),
                new Tuple3<>(1, 2, 9),
                new Tuple3<>(1, 2, 11),
                new Tuple3<>(1, 2, 13));
        DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource = environment
            .fromCollection(tuple3s);

        streamSource.keyBy(0).max(2).printToErr();

        environment.execute("keyBy demo");
    }
}
