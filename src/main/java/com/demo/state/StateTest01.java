package com.demo.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        streamExecutionEnvironment.setParallelism(4);

        streamExecutionEnvironment.enableCheckpointing(1000L);
        streamExecutionEnvironment.setStateBackend(
                new FsStateBackend("file:///Users/wangxiaolin/Downloads/test")
        );

        streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    if (string.contains("quit")){
                        throw new RuntimeException("java");
                    }
                    out.collect(Tuple2.of(string, 1));
                } }
        }).uid("test");

        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.getField(0);
            }
        }).sum(1);

        outputStreamOperator.print();


        streamExecutionEnvironment.execute();
    }
}
