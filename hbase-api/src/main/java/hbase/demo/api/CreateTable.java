package hbase.demo.api;

import com.google.common.collect.Lists;
import hbase.demo.utils.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * Created by liujiangtao1 on 15:16 2020-11-09.
 *
 * @Description:
 */
public class CreateTable {

    private static final String TABLE_NAME = "emp22";
    public static void main(String[] args) throws Exception {

        System.out.println("start");
        Connection connection = HBaseUtil.getConnection();

        TableDescriptorBuilder emp = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
        ColumnFamilyDescriptor personal = ColumnFamilyDescriptorBuilder.of("personal");
        ColumnFamilyDescriptor professional = ColumnFamilyDescriptorBuilder.of("professional");
        emp.setColumnFamilies(Lists.newArrayList(personal, professional));

        TableDescriptor empDescriptor = emp.build();

        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(TABLE_NAME));
        if (tableExists) {
            System.out.println("Table is exist");
        } else {
            admin.createTable(empDescriptor);
        }


        admin.close();
        System.out.println("end");


    }


}
