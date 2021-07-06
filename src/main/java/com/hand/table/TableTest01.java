package com.hand.table;

import com.hand.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.bridge.java.*;


public class TableTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();


        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = localhost.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(Integer.valueOf(split[0]),Double.valueOf(split[1]),Long.valueOf(split[2]));
            }
        });

        Table table = tableEnvironment.fromDataStream(map);
        tableEnvironment.createTemporaryView("watersensor",table);

        Table table1 = tableEnvironment.sqlQuery("select * from watersensor");

        DataStream<WaterSensor> waterSensorDataStream = tableEnvironment.toAppendStream(table1, WaterSensor.class);

        waterSensorDataStream.print();

        env.execute();
    }
}
