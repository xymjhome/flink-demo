package streamapi;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();
        ArrayList<Tuple3<Integer, Integer, Integer>> tuple3s = Lists
            .newArrayList(
                new Tuple3<>(0, 1, 4),
                new Tuple3<>(0, 1, 1),
                new Tuple3<>(0, 2, 2),
                new Tuple3<>(0, 1, 3),
                new Tuple3<>(1, 2, 5),
                new Tuple3<>(1, 2, 9),
                new Tuple3<>(1, 2, 11),
                new Tuple3<>(1, 2, 13));
        DataStreamSource<Tuple3<Integer, Integer, Integer>> streamSource = environment
            .fromCollection(tuple3s);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce = streamSource.keyBy(0)
            .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                @Override
                public Tuple3<Integer, Integer, Integer> reduce(
                    Tuple3<Integer, Integer, Integer> t2,
                    Tuple3<Integer, Integer, Integer> t1) throws Exception {
                    //return new Tuple3<>(0, 0, t2.f2 + t1.f2);
                    Tuple3<Integer, Integer, Integer> tuple3 = new Tuple3();
                    tuple3.setFields(0, 0, (int)t1.getField(2) + (int)t2.getField(2));
                    return tuple3;
                }
            });

        reduce.printToErr().setParallelism(1);

        environment.execute("reduce demo");

    }
}
