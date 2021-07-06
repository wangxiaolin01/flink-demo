package demo2.source;

import com.hand.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("file:///Users/wangxiaolin/Downloads/test"));

        // checkpoint 的清除策略
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig
                        .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置重启策略，/5次尝试/每次尝试间隔50s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,50000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = localhost.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] s = value.split(" ");
                return new WaterSensor(Integer.valueOf(s[0]), Double.valueOf(s[1]), Long.valueOf(s[2]));
            }
        });

        map.print();
//        Iterable<JobVertex> vertices = env.getStreamGraph().getJobGraph().getVertices();
//
//        for (JobVertex vertex : vertices) {
//            System.out.println("=======>"+vertex.getName());
//            System.out.println("=======>"+vertex.getID());
//        }

        env.execute(SocketSource.class.getCanonicalName());
    }
}
