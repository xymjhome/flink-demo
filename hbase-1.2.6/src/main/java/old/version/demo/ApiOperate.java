package old.version.demo;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by liujiangtao1 on 17:04 2020-11-11.
 *
 * @Description:
 */
public class ApiOperate {

    private static final String TABLE_NAME = "emp33";
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hbase-master");//主机名称
        //configuration.set("zookeeper.znode.parent", "/hbase");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(configuration);

//        create(connection);

        scan(connection);

        System.out.println("-----------------before--------------");

        insert(connection);

        scan(connection);

        connection.close();


    }

    private static void scan(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("rowKey=" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print(" family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print(" column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" value=" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();

    }

    private static void insert(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put test1 = new Put(Bytes.toBytes("test1"));
        test1.addColumn("personal".getBytes(),  "name".getBytes(), "zhangsan".getBytes());
        test1.addColumn("personal".getBytes(),  "city".getBytes(), "beijing".getBytes());
        test1.addColumn("professional".getBytes(),  "name".getBytes(), "lisi".getBytes());
        test1.addColumn("professional".getBytes(),  "city".getBytes(), "sh".getBytes());

        table.put(test1);
        table.close();
    }

    public static void create(Connection connection) throws IOException {
        HTableDescriptor tableDescriptor = new
            HTableDescriptor(TableName.valueOf(TABLE_NAME));
        HColumnDescriptor personal = new HColumnDescriptor(Bytes.toBytes("personal"));
        HColumnDescriptor professional = new HColumnDescriptor(Bytes.toBytes("professional"));
        tableDescriptor.addFamily(personal);
        tableDescriptor.addFamily(professional);

        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(TABLE_NAME));
        if (tableExists) {
            System.out.println("Table is exist");
        } else {
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }

}
