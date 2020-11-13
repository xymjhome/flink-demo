package hive.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

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
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/liujiangtao5/other/StuCode_OS/flink/flink-demo/hive-api/src/main/resources";

        //describe FORMATTED pokes;
        //alter table pokes set TBLPROPERTIES ('is_generic'='false');
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
        tableEnvironment.registerCatalog(catalogName, hiveCatalog);
        tableEnvironment.useCatalog(catalogName);

        Table sqlQuery = tableEnvironment.sqlQuery("select * from pokes");
        TableResult result = sqlQuery.execute();
        result.print();

    }
}
