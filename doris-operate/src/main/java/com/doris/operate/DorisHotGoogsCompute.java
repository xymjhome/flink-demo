package com.doris.operate;


import com.doris.utils.SQLUtil;
import com.doris.utils.SQLUtil.SQLStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DorisHotGoogsCompute {

    public static void main(String[] args) throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = SQLUtil.getConnection();
        if (null == connection) {
            log.error("get connection error");
            return;
        }


//        PreparedStatement firstMinTimestampStatement = connection.prepareStatement(SQLStatement.FIRST_MIN_TIMESTAMP);
//        ResultSet allMinTimeResult = firstMinTimestampStatement.executeQuery();
        Statement statement = connection.createStatement();
        ResultSet allMinTimeResult = statement.executeQuery(SQLStatement.FIRST_MIN_TIMESTAMP);

        long allMinTimestamp = 0;
        long allMaxTimestamp = 0;
        if (allMinTimeResult.next()) {
            allMinTimestamp = allMinTimeResult.getLong("min_timestamp");
            allMaxTimestamp = allMinTimeResult.getLong("max_timestamp");
        }
        allMinTimeResult.close();

        long distance = 60 * 60;
        long slide = 5 * 60;

        long maxTimestamp = allMinTimestamp + slide;
        long minTimestamp = maxTimestamp - distance;

        while (maxTimestamp <= allMaxTimestamp) {
            ResultSet minMaxTimeResult = statement.executeQuery(
                String.format(SQLStatement.MIN_MAX_TIMESTAMP, minTimestamp,  maxTimestamp));
            if (minMaxTimeResult.next()) {
                minTimestamp = minMaxTimeResult.getLong("min_timestamp");
                maxTimestamp = minMaxTimeResult.getLong("max_timestamp");
                minMaxTimeResult.close();
            } else {
                break;
            }

            if (minTimestamp <= 0) {
                break;
            }

            int topSize = 3;
            ResultSet hotItemsQuery = statement.executeQuery(
                String.format(SQLStatement.TOM_HOT_ITEMS, minTimestamp,  maxTimestamp, topSize));

            int number = 1;
            StringBuilder itemBuilder = new StringBuilder();
            while (hotItemsQuery.next() && number <= topSize) {
                long itemId = hotItemsQuery.getLong("item_id");
                long pvCount = hotItemsQuery.getLong("pv");
                itemBuilder.append("No:" + number).append("-->");
                itemBuilder.append("ItemId:" + itemId).append("-->");
                itemBuilder.append("Count:" + pvCount).append("\n");
                number++;
            }

            if (number > topSize) {
                StringBuilder result = new StringBuilder();
                result.append(StringUtils.repeat("*", 50)).append("\n");
                result.append("Time:").append(new Timestamp(maxTimestamp * 1000)).append("\n");
                result.append(itemBuilder.toString());
                result.append(StringUtils.repeat("*", 50)).append("\n");
                System.out.println(result.toString());
            }

            maxTimestamp += slide;
            minTimestamp = maxTimestamp - distance;
            hotItemsQuery.close();
        }

        statement.close();
        connection.close();

        long end = System.currentTimeMillis();
        System.out.println("use time:" + (end - start) + "ms");

    }

}
