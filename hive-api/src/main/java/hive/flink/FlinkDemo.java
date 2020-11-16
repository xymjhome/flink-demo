package hive.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * Created by liujiangtao1 on 11:35 2020-11-13.
 *
 * @Description:
 */
public class FlinkDemo {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode()
            .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String catalogName = "hive";
        String defaultDatabase = "mytestdb";
        String hiveConfDir = "hive-api/src/main/resources";

        //describe FORMATTED pokes;
        //alter table pokes set TBLPROPERTIES ('is_generic'='false');
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog(catalogName, hiveCatalog);
        tableEnvironment.useCatalog(catalogName);

        //todo have debug need fix
        //感觉是访问hdfs路径问题
        Table sqlQuery = tableEnvironment.sqlQuery("select * from employee2");
        TableResult result = sqlQuery.execute();
        result.print();

//        TableResult result1 = tableEnvironment
//            .executeSql("insert into employee2 values(100, 'test', '5000', 'destina_tion')");
//        result1.print();
//
//
//        Table sqlQuery2 = tableEnvironment.sqlQuery("select * from employee2");
//        TableResult result2 = sqlQuery2.execute();
//        result2.print();


    }
}
