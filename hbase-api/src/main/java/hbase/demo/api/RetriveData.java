package hbase.demo.api;

import hbase.demo.utils.Constants;
import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by liujiangtao1 on 10:35 2020-11-10.
 *
 * @Description:
 */
public class RetriveData {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME_TEST));

        for (int i = 0; i < 20; i++) {
            Get get = new Get(Bytes.toBytes(i + ""));
            Result result = table.get(get);
            byte[] name = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
            byte[] city = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
            System.out.println("row key:" + i + " name:" + Bytes.toString(name) + " city:" + Bytes.toString(city));

            System.out.println(StringUtils.repeat("-",20));

            List<Cell> columnCells = result
                .getColumnCells(Bytes.toBytes("personal"), Bytes.toBytes("city"));
            for (Cell cell : columnCells) {
                byte[] familyArray = cell.getFamilyArray();
                System.out.println("familyArray:" + Bytes.toString(familyArray));

                byte familyLength = cell.getFamilyLength();
                System.out.println("failyLength:" + familyLength);

                int familyOffset = cell.getFamilyOffset();
                System.out.println("familyOffset :" + familyOffset);

                byte[] qualifierArray = cell.getQualifierArray();
                System.out.println("qualifierArray:" + qualifierArray);

                int qualifierLength = cell.getQualifierLength();
                System.out.println("qualifierLength:" + qualifierLength);

                int qualifierOffset = cell.getQualifierOffset();
                System.out.println("qualifierOffset:" + qualifierOffset);

                byte[] rowArray = cell.getRowArray();
                System.out.println("rowArray:" + Bytes.toString(rowArray));

                short rowLength = cell.getRowLength();
                System.out.println("rowLength:" + rowLength);

                int rowOffset = cell.getRowOffset();
                System.out.println("rowOffset:" + rowOffset);

                long timestamp = cell.getTimestamp();
                System.out.println("timestamp:" + new Date(timestamp));

                Type type = cell.getType();
                System.out.println("type:" + type.toString());

                byte[] valueArray = cell.getValueArray();
                System.out.println("valueArray:" + Bytes.toString(valueArray));

                int valueLength = cell.getValueLength();
                System.out.println("valueLength:" + valueLength);

                int valueOffset = cell.getValueOffset();
                System.out.println("valueOffset:" + valueOffset);
            }
        }


        table.close();
        connection.close();
    }

}
