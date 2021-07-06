package com.demo.state;

import com.demo.bean.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MyState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost",9999);

        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                String[] strings = value.split(" ");
                out.collect(new SensorReading(strings[0], Double.valueOf(strings[1]), Long.valueOf(strings[2])));
            }
        });

        sensorReadingSingleOutputStreamOperator.keyBy(f -> f.getId())
                .map(new RichMapFunction<SensorReading, Tuple2<SensorReading,Double>>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListState<SensorReading> value = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("value", SensorReading.class));
                    }

                    @Override
                    public Tuple2<SensorReading, Double> map(SensorReading value) throws Exception {
                        return null;
                    }
                });

    }
}
