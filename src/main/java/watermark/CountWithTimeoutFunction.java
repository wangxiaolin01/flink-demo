//package watermark;
//
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.RichProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class CountWithTimeoutFunction extends RichProcessFunction<Tuple2<String,String>,Tuple2<String,Long>> {
//    private ValueState<CountWithTimestamp> state;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState",CountWithTimestamp.class));
//    }
//
//    @Override
//    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//        CountWithTimestamp current = state.value();
//        if (current == null){
//            current = new CountWithTimestamp();
//            current.key = value.f0;
//        }
//
//        current.count++;
//
//        current.lastModifield = ctx.timestamp();
//
//        state.update(current);
//
//        ctx.timerService().registerEventTimeTimer(current.lastModifield+6000);
//    }
//
//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//        CountWithTimestamp result = state.value();
//        System.out.println("onTimer: "+state.value());
//
//        if (timestamp== result.lastModifield+6000){
//            System.out.println("onTimer timeout:"+result.key);
//            out.collect(new Tuple2<String,Long>(result.key,result.count));
//        }
//    }
//}
