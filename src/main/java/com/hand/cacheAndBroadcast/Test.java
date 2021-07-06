package com.hand.cacheAndBroadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataSource = executionEnvironment.fromElements(1, 2, 3);

        executionEnvironment.socketTextStream("localhost",9999)
                .setParallelism(2).slotSharingGroup("test").disableChaining();   //设置slot共享组
           //对于不同的slot共享组，不同阶段的蒜子不能共享slot，对于相同的slot共享组，不同阶段的算子可以共享slot

    }
}
