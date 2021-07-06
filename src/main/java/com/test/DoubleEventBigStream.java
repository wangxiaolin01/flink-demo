//package com.test;
//
//import org.apache.flink.api.common.RuntimeExecutionMode;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.scala.function.WindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingAlignedProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//
//import java.math.BigDecimal;
//import java.time.Duration;
//
//public class DoubleEventBigStream {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//
//        DataStreamSource<Tuple2<String, BigDecimal>> tuple2DataStreamSource = streamExecutionEnvironment.addSource(new DoubleEventBigSource());
//
//        tuple2DataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
//        .withTimestampAssigner());
//
//        KeyedStream<Tuple2<String, BigDecimal>, Object> tuple2ObjectKeyedStream = tuple2DataStreamSource.keyBy(new KeySelector<Tuple2<String, BigDecimal>, Object>() {
//            @Override
//            public Object getKey(Tuple2<String, BigDecimal> value) throws Exception {
//                return value.f0;
//            }
//        });
//        //窗口从当地时间每天00:00开始，可以使用
////                .window(SlidingAlignedProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
////                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
//
//
//        tuple2ObjectKeyedStream.print();
//
//        streamExecutionEnvironment.execute();
//    }
//
//    static class PriceAggregate implements  AggregateFunction<Tuple2<String,BigDecimal>, BigDecimal, Tuple2<String,BigDecimal>>{
//
//        // 初始化累加器
//        @Override
//        public BigDecimal createAccumulator() {
//            return new BigDecimal("0.0");
//        }
//
//        @Override
//        public BigDecimal add(Tuple2<String, BigDecimal> value, BigDecimal accumulator) {
//            return null;
//        }
//
//        @Override
//        public Tuple2<String, BigDecimal> getResult(BigDecimal accumulator) {
//            return null;
//        }
//
//        @Override
//        public BigDecimal merge(BigDecimal a, BigDecimal b) {
//            return null;
//        }
//    }
//
//}
//
