package com.demo.tranform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class ConnecTranform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        streamExecutionEnvironment.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.fromElements("java", "scala", "python");
        DataStreamSource<Integer> integerDataStreamSource = streamExecutionEnvironment.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringDataStreamSource1 = streamExecutionEnvironment.fromElements("wahah", "scala", "java");

        ConnectedStreams<String, Integer> connect = stringDataStreamSource.connect(integerDataStreamSource);
        SingleOutputStreamOperator<Object> map = connect.map(new RichCoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String s) throws Exception {
                return "String--" + s;
            }

            @Override
            public Object map2(Integer integer) throws Exception {
                return "Integer--" + integer;
            }
        });

        map.print();


        streamExecutionEnvironment.execute();

    }
}
