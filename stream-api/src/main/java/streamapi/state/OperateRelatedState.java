package streamapi.state;


import java.sql.Types;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streamapi.pojo.Sensor;
import streamapi.util.ParseSourceDataUtil;

public class OperateRelatedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Sensor> source = ParseSourceDataUtil
            .getSensorSocketSource(env);

        SingleOutputStreamOperator<String> stateResult = source
            .keyBy(sensor -> sensor.getId())
            .flatMap(new TemperatureAlertFunction(2.0));


        stateResult.printToErr();

        env.execute("operate state");
    }

    private static class TemperatureAlertFunction extends RichFlatMapFunction<Sensor, String> {

        private double threshold;

        private ValueState<Double> lastTemperature;

        public TemperatureAlertFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemperature = getRuntimeContext().getState(
                new ValueStateDescriptor<Double>("lastTemperature", Double.class));
        }

        @Override
        public void flatMap(Sensor value, Collector<String> out) throws Exception {
            Double preTemperature = lastTemperature.value();
            if (null == preTemperature) {
                lastTemperature.update(value.getTemperature());
                return;
            }

            double curTemperature = value.getTemperature();
            double diff = Math.abs(curTemperature - preTemperature);
            if (diff >= threshold) {
                out.collect(value.getId()
                    + " temperature diff:" + diff + " more than threshold("
                    + threshold + ")");
            }

            lastTemperature.update(curTemperature);
        }
    }
}
