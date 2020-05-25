package streamapi.sink;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import streamapi.pojo.Sensor;
import streamapi.util.ParseFileDataUtil;

public class MysqlSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        SingleOutputStreamOperator<Sensor> sensor =
            ParseFileDataUtil.getSensorFileSourcer(env, "sensor_data.txt");


        sensor.addSink(new MysqlJdbcSink());

        env.execute("mysql sink");
    }

    public static class MysqlJdbcSink extends RichSinkFunction<Sensor> {

        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sensor",
                "root", "123456");
            insertStatement = connection.prepareStatement("INSERT INTO sensor_test(id, temperature) VALUES (?, ?)");
            updateStatement = connection.prepareStatement("UPDATE sensor_test SET temperature = ? WHERE id = ?");
        }


        @Override
        public void invoke(Sensor value, Context context) throws Exception {
            updateStatement.setString(2, value.getId());
            updateStatement.setDouble(1, value.getTemperature());

            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, value.getId());
                insertStatement.setDouble(2, value.getTemperature());
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }

    }
}
