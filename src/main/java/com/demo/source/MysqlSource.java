package com.demo.source;

import com.demo.bean.SensorReading;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class MysqlSource {
}

class  CustomMysqlSource extends RichSourceFunction<String> {

    private ObjectMapper objectMapper = new ObjectMapper();

    private String URL= "jdbc:mysql://localhost:3306/test";
    private String PASSWD = "test";
    private String USERNAME = "test";

    private Connection connection = null;
    private PreparedStatement statement = null;
    ResultSet resultSet = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(URL,USERNAME,PASSWD);
        statement = connection.prepareStatement("select id,temperature,timestamp from test");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        resultSet = statement.executeQuery();
        while (resultSet.next()){
            String id = resultSet.getString("id");
            Double temperature = resultSet.getDouble("temperature");
            Long timestamp = resultSet.getLong("timestamp");
            SensorReading sensorReading = new SensorReading(id,temperature,timestamp);

            //转换为json对象输出
            ctx.collect(objectMapper.writeValueAsString(sensorReading));
        }
    }

    /**
     * 流关闭操作
     */
    @Override
    public void cancel() {
        if (resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if (statement != null){
            try {
                statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if (connection!=null){
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

}
