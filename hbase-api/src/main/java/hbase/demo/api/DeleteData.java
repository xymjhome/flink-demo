package hbase.demo.api;

import hbase.demo.utils.Constants;
import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by liujiangtao1 on 10:57 2020-11-10.
 *
 * @Description:
 */
public class DeleteData {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME_TEST));

        scanner(table);
        System.out.println("--------before-------------");

        Delete delete = new Delete(Bytes.toBytes("1"));

        table.delete(delete);

        scanner(table);

        table.close();
        connection.close();

    }

    public static void scanner(Table table) throws IOException {
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            Cell[] cells = next.rawCells();
            for (Cell cell : cells) {
                byte[] rowKey = CellUtil.cloneRow(cell);
                System.out.print("rowKey:" + Bytes.toString(rowKey));

                byte[] family = CellUtil.cloneFamily(cell);
                System.out.print(" family:" + Bytes.toString(family));

                byte[] column = CellUtil.cloneQualifier(cell);
                System.out.print(" column:" + Bytes.toString(column));

                byte[] value = CellUtil.cloneValue(cell);
                System.out.println(" value :" + Bytes.toString(value));
            }
//            byte[] name = next.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
//            byte[] city = next.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
//
//            System.out.println("name:" + Bytes.toString(name) + " city:" + Bytes.toString(city));
        }
    }
}
