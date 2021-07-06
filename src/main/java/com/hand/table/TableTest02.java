package com.hand.table;

import com.hand.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.currentTime;

public class TableTest02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);

        SingleOutputStreamOperator<WaterSensor> localhost = env.socketTextStream("localhost", 8888)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(Integer.valueOf(split[0]), Double.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                });

//        Table table = tableEnvironment.fromDataStream(localhost, "id,temperature,timestamp.rowtime");


        Table table = tableEnvironment.fromDataStream(localhost, $("id"), $("temperature"), $("timestamp").rowtime(),$("ts").proctime());



        tableEnvironment.toAppendStream(table, Row.class).print();


        env.execute();
    }
}
