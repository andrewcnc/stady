package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class Main {
    private static String URL_SIMPLE_DB$ = "jdbc:sqlite:/home/andy/NewFolder/simple_database.db";
    private static String URL_MAGIC_DB$ = "jdbc:sqlite:/home/andy/NewFolder/magic_database.db";
    private static String URL_CSV_SOURCE$ = "/home/andy/NewFolder/ALUMINIUM";
    public static void main(String[] args) {
        System.out.println("simple:" + createSimpleDatabase());
        System.out.println("magic:" + createMagicDatabase());


    }
    private static long createMagicDatabase(){
        Instant start = Instant.now();
        File fileCSV = new File(URL_CSV_SOURCE$);
        final String tableName$ = "magic" + fileCSV.getName().toLowerCase().split("\\.")[0];
        String createTable$ = "CREATE TABLE IF NOT EXISTS " + tableName$ + " (" +
                "diameter REAL NOT NULL UNIQUE, " +
                "max_rpm INTEGER NOT NULL, " +
                "feed_per_tooth REAL NOT NULL, " +
                "teeth_count INTEGER NOT NULL, " +
                "feed_rate REAL NOT NULL, " +
                "PRIMARY KEY (diameter)" +
                ");";
        String sqlInput$ = "INSERT INTO " + tableName$ +
                " (diameter, max_rpm, feed_per_tooth, teeth_count, feed_rate)" +
                " VALUES (?, ?, ?, ?, ?)";
        try(Connection connection = DriverManager.getConnection(URL_MAGIC_DB$))
        {
            connection.setAutoCommit(false);
            ArrayList<String> list = new ArrayList<>(Files.readAllLines(Path.of(URL_CSV_SOURCE$), StandardCharsets.UTF_8));
            Statement statement = connection.createStatement();
            statement.execute(createTable$);
            PreparedStatement preparedStatement = connection.prepareStatement(sqlInput$);
            String[] arr;
            String diameter$;
            Double diameter;
            String max_rpm$;
            Integer max_rpm;
            String feed_per_tooth$;
            Double feed_per_tooth;
            String teeth_count$;
            Integer teeth_count;
            String feed_rate$;
            Double feed_rate;

            for (int i = 1; i < list.size(); i++) {
                arr = list.get(i).split(";");
                diameter$ = arr[0].replace(",", ".");
                max_rpm$ = arr[1];
                feed_per_tooth$ = arr[2].replace(",", ".");
                teeth_count$ = arr[3];
                feed_rate$ = arr[4].replace(",", ".");
                diameter = Double.parseDouble(diameter$);
                max_rpm = Integer.parseInt(max_rpm$);
                feed_per_tooth = Double.parseDouble(feed_per_tooth$);
                teeth_count = Integer.parseInt(teeth_count$);
                feed_rate = Double.parseDouble(feed_rate$);
//                String sqlInput$ = "INSERT INTO " + tableName$ +
//                        " (diameter, max_rpm, feed_per_tooth, teeth_count, feed_rate)" +
//                        " VALUES (?, ?, ?, ?, ?)";
                    preparedStatement.setDouble(1, diameter);
                    preparedStatement.setInt(2, max_rpm);
                    preparedStatement.setDouble(3, feed_per_tooth);
                    preparedStatement.setInt(4, teeth_count);
                    preparedStatement.setDouble(5, feed_rate);

                }
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            connection.commit();


    } catch (SQLException | IOException e) {
            throw new RuntimeException(e);

        }
        Instant end = Instant.now();
        return Duration.between(start, end).toMillis();
    }
    private static long createSimpleDatabase(){
        Instant start = Instant.now();
        File fileCSV = new File(URL_CSV_SOURCE$);
        final String tableName$ = "simple" + fileCSV.getName().toLowerCase().split("\\.")[0];
        String createTable$ = "CREATE TABLE IF NOT EXISTS " + tableName$ + " (" +
                "diameter REAL NOT NULL UNIQUE, " +
                "max_rpm INTEGER NOT NULL, " +
                "feed_per_tooth REAL NOT NULL, " +
                "teeth_count INTEGER NOT NULL, " +
                "feed_rate REAL NOT NULL, " +
                "PRIMARY KEY (diameter)" +
                ");";
        try(Connection connection = DriverManager.getConnection(URL_SIMPLE_DB$))
        {
            ArrayList<String> list = new ArrayList<>(Files.readAllLines(Path.of(URL_CSV_SOURCE$), StandardCharsets.UTF_8));
            Statement statement = connection.createStatement();
            statement.execute(createTable$);
            String[] arr;
            String diameter$;
            Double diameter;
            String max_rpm$;
            Integer max_rpm;
            String feed_per_tooth$;
            Double feed_per_tooth;
            String teeth_count$;
            Integer teeth_count;
            String feed_rate$;
            Double feed_rate;

            for (int i = 1; i < list.size(); i++) {
                arr = list.get(i).split(";");
                diameter$ = arr[0].replace(",", ".");
                max_rpm$ = arr[1];
                feed_per_tooth$ = arr[2].replace(",", ".");
                teeth_count$ = arr[3];
                feed_rate$ = arr[4].replace(",", ".");
                diameter = Double.parseDouble(diameter$);
                max_rpm = Integer.parseInt(max_rpm$);
                feed_per_tooth = Double.parseDouble(feed_per_tooth$);
                teeth_count = Integer.parseInt(teeth_count$);
                feed_rate = Double.parseDouble(feed_rate$);
                String sqlInput$ = "INSERT INTO " + tableName$ +
                        " (diameter, max_rpm, feed_per_tooth, teeth_count, feed_rate)" +
                        " VALUES (" + diameter + " " + ", " + max_rpm +  ", " +feed_per_tooth + ", " +teeth_count +  ", " +feed_rate + ")";
                statement.executeUpdate(sqlInput$);
                //connection.commit();
            }

        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);

        }
        Instant end = Instant.now();
        return Duration.between(start, end).toMillis();
    }
}