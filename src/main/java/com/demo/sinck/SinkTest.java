package com.demo.sinck;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class SinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<Long> longDataStreamSource = env.fromSequence(0, 1000);

//       longDataStreamSource.addSink(JdbcSink.sink("",
//               ((preparedStatement, aLong) -> {
//                   preparedStatement.setTime(1,aLong.longValue());
//               },
//               new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//               .withDriverName("com.mysql.jdbc")
//               .withUrl("")
//               )));
        longDataStreamSource.addSink(StreamingFileSink
                .forRowFormat(new Path(""),new SimpleStringEncoder<Long>("UTF-8"))
                .build());

    }
}
