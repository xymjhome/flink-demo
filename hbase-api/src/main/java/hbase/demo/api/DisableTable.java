package hbase.demo.api;

import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by liujiangtao1 on 18:06 2020-11-09.
 *
 * @Description:
 */
public class DisableTable {
    private static final String TABLE_NAME = "emp";
    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Admin admin = connection.getAdmin();
        boolean tableDisabled = admin.isTableDisabled(TableName.valueOf(TABLE_NAME));
        System.out.println(TABLE_NAME + " is disabled:" + tableDisabled);
        if (!tableDisabled) {
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            System.out.println("disable table:" + TABLE_NAME + "sucess");
        }
    }
}
