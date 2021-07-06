package com.hand;

import com.hand.EnumBase.EnumBase;
import com.hand.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

public class FlinkTest01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment  streamExecutionEnvironment = new StreamExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        OutputTag<WaterSensor> waterSensorOutputTag = new OutputTag<>("test");

        DataStreamSource<String> socketStream = streamExecutionEnvironment.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<WaterSensor> mapDataStream= socketStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] strings = s.split(",");
                return new WaterSensor(Integer.valueOf(strings[0]), Double.valueOf(strings[1]), Long.valueOf(strings[2]));
            }
        });

        mapDataStream.keyBy(data -> data.getId())
                .timeWindow(Time.milliseconds(10), Time.milliseconds(5))
                .allowedLateness(Time.minutes(5))
                .sideOutputLateData(waterSensorOutputTag);
    }
}
