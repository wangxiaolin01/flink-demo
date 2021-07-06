package com.demo.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import java.util.List;

public class ConsoleSource {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String output = "";
        if (parameterTool.has("output")){
            output = "";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
//        DataSet<String> localhost = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("java","java", "scala", "waha");
        stringDataStreamSource.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> s = stringDataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Object> tuple2ObjectKeyedStream = s.keyBy(f -> f.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2ObjectKeyedStream.sum(1);

        sum.print();

        System.setProperty("HADOOP_USER_NAME","root");

        env.execute();

    }
}
