package hive.metastore;

import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FieldSchema._Fields;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * Created by liujiangtao1 on 17:23 2020-11-12.
 *
 * @Description:
 */
public class MetaStoreDemo {

    public static void main(String[] args) throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource("hive-site.xml");

        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        //getTableInfo(client);

        getDatabase(client);

        client.close();
    }

    public static void getDatabase(HiveMetaStoreClient client) throws Exception {
        List<String> allDatabases = client.getAllDatabases();
        System.out.println("allDatabases:" + allDatabases);
        for (String dbName : allDatabases) {
            List<String> allTables = client.getAllTables(dbName);
            for (String tableName : allTables) {
                Table table = client.getTable(dbName, tableName);
                List<String> bucketCols = table.getSd().getBucketCols();
                System.out.println("bucketCols:" + bucketCols);
                String location = table.getSd().getLocation();
                System.out.println("location:" + location);
                List<FieldSchema> cols = table.getSd().getCols();
                for (FieldSchema schema: cols) {
                    System.out.println("schema name: " + schema.getName() + ", type: " + schema.getType());
                }
                int createTime = table.getCreateTime();
                System.out.println("createTime:" + createTime);

                String owner = table.getOwner();
                System.out.println("owner:" + owner);
            }
        }

    }

    public static void getTableInfo(HiveMetaStoreClient client) throws TException {
        //获取数据库信息
        List<String> tablesList = client.getAllTables("default");
        System.out.print("test数据所有的表:  ");
        for (String s : tablesList) {
            System.out.print(s + "\t");
        }
        System.out.println();

        //获取表信息
        System.out.println("default.pokes 表信息: ");
        Table table= client.getTable("default","pokes");
        List<FieldSchema> fieldSchemaList= table.getSd().getCols();
        for (FieldSchema schema: fieldSchemaList) {
            System.out.println("字段: " + schema.getName() + ", 类型: " + schema.getType());
            Object fieldValue = schema.getFieldValue(_Fields.COMMENT);
            System.out.println(fieldValue);
        }

        Object fieldValue = table.getFieldValue(Table._Fields.DB_NAME);
        System.out.println(fieldValue);
    }
}
