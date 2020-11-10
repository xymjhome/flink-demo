package hbase.demo.api;

import hbase.demo.utils.Constants;
import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by liujiangtao1 on 20:46 2020-11-09.
 *
 * @Description:
 */
public class UpdateData {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME_TEST));

        Get get = new Get(Bytes.toBytes("12"));
        get.addFamily(Bytes.toBytes("personal"));

        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
        System.out.println("before 12 personal city:" + Bytes.toString(value));

        Put put = new Put("12".getBytes());
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("update_data_henan"));


        table.put(put);

        Result result1 = table.get(get);
        byte[] value1 = result1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
        System.out.println("after 12 personal city:" + Bytes.toString(value1));
        table.close();
        connection.close();
    }

}
