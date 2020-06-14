package com.doris.operate;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DorisOpreateDemo {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager
            .getConnection("jdbc:mysql://127.0.0.1:9030/example_db", "root", null);

        Statement statement = connection.createStatement();
        String sqlQuery = "SELECT user_id,item_id,behavior FROM user_behavior limit 10";

        //A <code>ResultSet</code> cursor is initially positioned
        //before the first row; the first call to the method
        //<code>next</code> makes the first row the current row; the
        //second call makes the second row the current row, and so on.
        ResultSet resultSet = statement.executeQuery(sqlQuery);

        while (resultSet.next()) {

            long userId = resultSet.getLong("user_id");
            long itemId = resultSet.getLong("item_id");
            String behavior = resultSet.getString("behavior");

            System.out.println(userId + " | " + itemId + " | " + behavior);
        }

        statement.close();
        resultSet.close();
        connection.close();

    }
}
