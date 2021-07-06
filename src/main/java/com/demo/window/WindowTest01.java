package com.demo.window;

import com.demo.bean.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class WindowTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost",9999);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                String[] strings = value.split(" ");
                out.collect(new SensorReading(strings[0], Double.valueOf(strings[1]), Long.valueOf(strings[2])));
            }
        });

        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator1 = sensorReadingSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5)) //指定maxOutOfBounderness最大无须时间/最大延迟时间
                        .withTimestampAssigner((order, timestamp) -> order.getTimestamp()));//指定时间时间列

        SingleOutputStreamOperator<String> apply = sensorReadingSingleOutputStreamOperator1.keyBy(data -> data.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<String> out) throws Exception {
                        List<String> list = new ArrayList<>();    //用来存放当前窗口的数据格式化后的时间
                        for (SensorReading sensorReading : input) {
                            long eventTime = sensorReading.getTimestamp();
                            String formatEventTime = simpleDateFormat.format(eventTime);
                            list.add(formatEventTime);
                        }
                        long start = window.getStart();
                        long end = window.getEnd();
                        //获得当前窗口的开始时间和结束时间，以及属于该窗口的所有数据的事件事件，拼接，并返回

                        String result = String.format("key:%s,窗口时间:[%s-%s],属于该窗口的时间为：",
                                s, start, end, list);
                        out.collect(result);
                    }
                });

        apply.print();

        env.execute();
    }


}
