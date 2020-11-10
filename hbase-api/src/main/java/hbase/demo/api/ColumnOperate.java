package hbase.demo.api;

import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by liujiangtao1 on 18:17 2020-11-09.
 *
 * @Description:
 */
public class ColumnOperate {

    private static final String TABLE_NAME = "emp";
    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        //describe 'emp' 命令查看列簇状态
        Admin admin = connection.getAdmin();
//        addColumn(admin);
        deleteColumn(admin);
    }

    private static void deleteColumn(Admin admin) throws IOException {
        admin.deleteColumnFamily(TableName.valueOf(TABLE_NAME), "personal".getBytes());
    }

    private static void addColumn(Admin admin) throws IOException {
        admin.addColumnFamily(TableName.valueOf(TABLE_NAME), ColumnFamilyDescriptorBuilder.of("test_column_family"));
    }

}
