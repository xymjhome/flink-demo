package hbase.demo.api;

import hbase.demo.utils.Constants;
import hbase.demo.utils.HBaseUtil;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by liujiangtao1 on 18:27 2020-11-09.
 *
 * @Description:
 */
public class DeleteTable {


    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getConnection();

        Admin admin = connection.getAdmin();
//        admin.deleteTable(TableName.valueOf(Constants.TABLE_NAME_TEST));//不可以直接删除，需要先使表变为不可用状态
        if (!admin.isTableDisabled(TableName.valueOf(Constants.TABLE_NAME_TEST))) {

            admin.disableTable(TableName.valueOf(Constants.TABLE_NAME_TEST));
        }
        admin.deleteTable(TableName.valueOf(Constants.TABLE_NAME_TEST));

    }

}
