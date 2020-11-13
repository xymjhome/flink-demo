package hbase.demo.utils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Created by liujiangtao1 on 17:56 2020-11-09.
 *
 * @Description:
 */
public class HBaseUtil {

    public static Connection getConnection() throws IOException {
        //配置连接信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hbase-region");//主机名称
        configuration.set("zookeeper.znode.parent", "/hbase");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        //指定用户
//        UserGroupInformation.setConfiguration(configuration);
//        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser("root");
//        UserGroupInformation.setLoginUser(remoteUser);

        //创建连接
        return ConnectionFactory.createConnection(configuration);
    }
}
