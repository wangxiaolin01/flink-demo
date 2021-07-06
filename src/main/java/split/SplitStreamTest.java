package split;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class SplitStreamTest {

    final static OutputTag<String> outputTag = new OutputTag<String>("side-output>5");
    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("kafka.bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","side");

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>("side",new SimpleStringSchema(),properties);


        /**
         * flink1.12 废弃了split算子，要切分流，可以用process function做
         */
        SingleOutputStreamOperator<Integer> lower = streamExecutionEnvironment.addSource(stringFlinkKafkaConsumer)
                .map(new String2Integer())
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        ctx.output(outputTag, "lower");
                    }
                });
    }

    public static class String2Integer extends RichMapFunction<String,Integer>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public Integer map(String value) throws Exception {
            return Integer.valueOf(value);
        }
    }
}
