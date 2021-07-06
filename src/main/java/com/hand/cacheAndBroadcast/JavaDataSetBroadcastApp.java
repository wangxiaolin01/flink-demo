package com.hand.cacheAndBroadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class JavaDataSetBroadcastApp {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //1. 初始化需要广播的数据集
        DataSet<Integer>  toBroadcast = env.fromElements(1,2,3);
        DataSet<String> data = env.fromElements("a","b");

        MapOperator<String, String> stringStringMapOperator = data.map(new RichMapFunction<String, String>() {
            private List<Integer> mlsit = null;

            //获取广播的数据集
            @Override
            public void open(Configuration parameters) throws Exception {
                List<Integer> broadCaseSetName = getRuntimeContext().getBroadcastVariable("broadCaseSetName");
                mlsit = broadCaseSetName;

            }

            @Override
            public String map(String s) throws Exception {
                return null;
            }
            //2. withBroadcastSet 广播数据
        }).withBroadcastSet(toBroadcast, "broadCaseSetName");

        stringStringMapOperator.print();
        env.execute();
    }
}
