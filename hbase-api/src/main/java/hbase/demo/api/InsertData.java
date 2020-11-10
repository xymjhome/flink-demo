package hbase.demo.api;

import hbase.demo.utils.Constants;
import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by liujiangtao1 on 18:34 2020-11-09.
 *
 * @Description:
 */
public class InsertData {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME_TEST));

        Put put = new Put("14".getBytes());
        put.addColumn("personal".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.addColumn("personal".getBytes(), "city".getBytes(), "beijing".getBytes());
        Put put2 = new Put("12".getBytes());
        put2.addColumn("professional".getBytes(), "name".getBytes(), "lisi".getBytes());
        put2.addColumn("professional".getBytes(), "city".getBytes(), "sh".getBytes());
        put2.addColumn("professional".getBytes(), "phone".getBytes(), "101111".getBytes());

//        table.put(put);
//        table.put(put2);

        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("rowKey=" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print(" family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print(" column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" value=" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        connection.close();
    }

}
