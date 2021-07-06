package state;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

public class CounterSource  extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
    private Long offset;

    private volatile  boolean isRunning = true;


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final  Object lock = ctx.getCheckpointLock();

        while (isRunning){
            synchronized (lock){
                ctx.collect(offset);
                offset+=1;
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
