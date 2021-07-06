package demo2.watermark;

import demo2.watermark.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<WaterSensor> stringDataStreamSource = env.addSource(new MySource());
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = stringDataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(WaterSensor element) {
                return element.getTimestamp() * 1000;
            }
        });


        waterSensorSingleOutputStreamOperator.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });


        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("filter", TypeInformation.of(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> filter = stringDataStreamSource.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getTemperature() > 38) {
                    ctx.output(outputTag,value);
                }
            }
        });

        DataStream<WaterSensor> sideOutput = filter.getSideOutput(outputTag);
        sideOutput.print();

        env.execute();

    }
}
