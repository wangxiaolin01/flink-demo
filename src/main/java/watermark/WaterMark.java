//package watermark;
//
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.watermark.Watermark;
//
//import javax.annotation.Nullable;
//
//public class WaterMark {
//    public static void main(String[] args) {
//        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamExecutionEnvironment.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
//
//        DataStreamSource<String> localhost = streamExecutionEnvironment.socketTextStream("localhost", 9999);
//
//        SingleOutputStreamOperator<Tuple2<String, Long>> process = localhost.map(new Split2KV())
//                .keyBy(new KeySelector<Tuple2<String, String>, Object>() {
//                    @Override
//                    public Object getKey(Tuple2<String, String> value) throws Exception {
//                        return value.f0;
//                    }
//                }).process(new CountWithTimeoutFunction());
//
//        process.print();
//
//    }
//
//
//    static public class Split2KV extends RichMapFunction<String, Tuple2<String,String>>{
//
//        @Override
//        public Tuple2<String, String> map(String value) throws Exception {
//            String[] s = value.split(" ");
//            return new Tuple2<>(s[0],s[1]);
//        }
//    }
//    private static class CustomWatermakrExtractor implements AssignerWithPeriodicWatermarks<String>{
//        private long currentTimestamp = Long.MIN_VALUE;
//
//        @Nullable
//        @Override
//        public Watermark getCurrentWatermark() {
//            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE:currentTimestamp - 1);
//        }
//
//        @Override
//        public long extractTimestamp(String element, long recordTimestamp) {
//            currentTimestamp = System.currentTimeMillis();
//            return currentTimestamp;
//        }
//    }
//}
//
