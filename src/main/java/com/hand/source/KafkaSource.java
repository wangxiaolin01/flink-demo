package com.hand.source;

import akka.stream.impl.ReducerState;
import com.hand.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(true);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.23.16.57:6667,172.23.16.58:6667,172.23.16.59:6667");
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        DataStreamSource<String> localhosy = env.socketTextStream("localhost", 9999);

        DataStreamSink<String> dataStreamSink = localhosy.addSink(new
                FlinkKafkaProducer<String>("laowang_test01", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<WaterSensor> map = localhosy.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(Integer.valueOf(split[0]),Double.valueOf(split[1]),Long.valueOf(split[2]));
            }
        });

        map.keyBy(data -> data.getId())
                .process(new KeyedProcessFunction<Integer, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }
                });

        env.execute();
    }
}



//class MyReduceFunction extends RichReduceFunction<WaterSensor>{
//    private ReducerState<WaterSensor,WaterSensor> reducerState;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        reducerState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Object>("reducestate",WaterSensor.class))
//    }
//
//    @Override
//    public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
//        return null;
//    }
//}