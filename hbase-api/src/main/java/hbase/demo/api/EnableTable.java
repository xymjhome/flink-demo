package hbase.demo.api;

import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by liujiangtao1 on 18:10 2020-11-09.
 *
 * @Description:
 */
public class EnableTable {
    private static final String TABLE_NAME = "emp";
    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Admin admin = connection.getAdmin();
        boolean tableEnabled = admin.isTableEnabled(TableName.valueOf(TABLE_NAME));
        System.out.println(TABLE_NAME + " is enabled:" + tableEnabled);
        if (!tableEnabled) {
            admin.enableTable(TableName.valueOf(TABLE_NAME));
            System.out.println("enable table:" + TABLE_NAME + "sucess");
        }
    }
}
