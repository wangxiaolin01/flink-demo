package com.mmain;

import com.hand.bean.WaterSensor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.formats.raw.RawFormatSerializationSchema;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = streamExecutionEnvironment.addSource(new GussionTest());
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(WaterSensor element) {
                return element.getTimestamp() * 1000;
            }
        });

        DataStream<WaterSensor> broadcast = waterSensorDataStreamSource.broadcast();
        KeyedStream<WaterSensor, Tuple2<Integer, Long>> waterSensorTuple2KeyedStream = waterSensorSingleOutputStreamOperator.keyBy(new KeySelector<WaterSensor, Tuple2<Integer, Long>>() {

            @Override
            public Tuple2<Integer, Long> getKey(WaterSensor value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTimestamp());
            }
        });

        waterSensorTuple2KeyedStream
                .timeWindow(Time.seconds(10))
//                .reduce()
                .apply(new WindowFunction<WaterSensor, Object, Tuple2<Integer, Long>, TimeWindow>() {
                    @Override
                    public void apply(Tuple2<Integer, Long> integerLongTuple2, TimeWindow window, Iterable<WaterSensor> input, Collector<Object> out) throws Exception {
                        window.getStart();
                        int size = IteratorUtils.toList(input.iterator()).size();
                        System.out.println(size);
                    }
                });


//        waterSensorTuple2KeyedStream.addSink(new FlinkKafkaProducer<WaterSensor>("localhost:9092","test",new RawFormatSerializationSchema()));
//        waterSensorDataStreamSource.print();
        streamExecutionEnvironment.execute();

    }
}
