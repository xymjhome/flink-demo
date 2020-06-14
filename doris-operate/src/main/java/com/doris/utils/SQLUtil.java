package com.doris.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLUtil {

    private static String MYSQL_CONF_FILE_NAME = "mysql.conf";
    private static String MYSQL_URL = "url";
    private static String MYSQL_URL_DEFAULT_VALUE = "jdbc:mysql://127.0.0.1:9030/example_db";
    private static String MYSQL_USER = "user";
    private static String MYSQL_USER_DEFAULT_VALUE = "root";
    private static String MYSQL_PASSWORD = "password";
    private static String MYSQL_PASSWORD_DEFAULT_VALUE = "";


    private static Config sqlConfig = null;
    static {
        File mysqlConf = new File(SQLUtil.class.getClassLoader()
            .getResource(MYSQL_CONF_FILE_NAME).getPath());
        sqlConfig = ConfigFactory.parseFile(mysqlConf).resolve();
    }

    public static Connection getConnection() {
        String url = sqlConfig.hasPath(MYSQL_URL) ? sqlConfig.getString(MYSQL_URL)
                : MYSQL_URL_DEFAULT_VALUE;
        String user = sqlConfig.hasPath(MYSQL_USER) ? sqlConfig.getString(MYSQL_USER)
            : MYSQL_USER_DEFAULT_VALUE;
        String password = sqlConfig.hasPath(MYSQL_PASSWORD) ? sqlConfig.getString(MYSQL_PASSWORD)
            : MYSQL_PASSWORD_DEFAULT_VALUE;

        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            log.error("get mysql connection error:", e);
        }

        return null;
    }

    public static class SQLStatement {
        public static String FIRST_MIN_TIMESTAMP = "select min(event_timestamp) min_timestamp,max(event_timestamp) max_timestamp from user_behavior";
        public static String MIN_MAX_TIMESTAMP = "select min(event_timestamp) min_timestamp,max(event_timestamp) max_timestamp "
            + "from user_behavior "
            + "where event_timestamp between %s and %s";
        public static String TOM_HOT_ITEMS = "select item_id,sum(pv) as pv "
            + "from user_behavior "
            + "where event_timestamp between %s and %s "
            + "group by item_id "
            + "order by pv desc limit %s";
    }
}
