package com.demo.tranform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.util.Collector;

public class OutputTagTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStreamSource<Double> doubleDataStreamSource = env.fromElements(36.7, 35.7, 37.5, 38.0);
        OutputTag<Double> lower = new OutputTag<>("lower", TypeInformation.of(Double.class));
        OutputTag<Double> high = new OutputTag<>("high", TypeInformation.of(Double.class));
        SingleOutputStreamOperator<Double> process = doubleDataStreamSource.process(new ProcessFunction<Double, Double>() {
            @Override
            public void processElement(Double aDouble, Context context, Collector<Double> collector) throws Exception {
                if (aDouble >= 37) {
                    context.output(high, aDouble);
                } else {
                    context.output(lower, aDouble);
                }
            }
        });

        process.getSideOutput(lower).print();

        env.execute();
    }
}
