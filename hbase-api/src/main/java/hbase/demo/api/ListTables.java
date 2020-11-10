package hbase.demo.api;

import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * Created by liujiangtao1 on 18:00 2020-11-09.
 *
 * @Description:
 */
public class ListTables {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Admin admin = connection.getAdmin();
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        tableDescriptors.forEach(tableDescriptor -> {
            System.out.println("table name:" + tableDescriptor.getTableName());
            ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
                System.out.println("table column family:" + columnFamily.getNameAsString());
            }
        });

        admin.close();
    }

}
