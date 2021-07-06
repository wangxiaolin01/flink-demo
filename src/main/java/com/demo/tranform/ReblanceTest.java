package com.demo.tranform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.util.Collector;

public class ReblanceTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStreamSource<Long> longDataStreamSource = env.fromSequence(0, 1000);

        //没有经过rebalance，数据可能发生倾斜
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> noRebalanceSum = longDataStreamSource.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();   //子任务id/分区编号
                return Tuple2.of(indexOfThisSubtask, 1);
            }
        })      //按照子任务ID/分区编号分组，并统计每个子任务/分区中有几个元素
                .keyBy(t -> t.f0).sum(1);


        //调用rebalance，解决数据倾斜
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> hasRebalanceSum = longDataStreamSource.rebalance().
                map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();   //子任务id/分区编号
                return Tuple2.of(indexOfThisSubtask, 1);
            }
        })      //按照子任务ID/分区编号分组，并统计每个子任务/分区中有几个元素
                .keyBy(t -> t.f0).sum(1);


    }
}
