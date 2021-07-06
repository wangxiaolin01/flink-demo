package com.demo.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class OperateStateTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //checkpoint
        env.enableCheckpointing(1000l);   // 每隔1s进行一次checkpoint
        //类型2：建议设置
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);  // 强后两次checkpoint最小间隔
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);  // checkpoint粗线错误，是否让整体任务失败（已过时，用setTolerableCheckpointFailureNumber）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);  // 容忍checkpoint失败的次数（默认0，表示不允许任何失败）

        env.setStateBackend(new FsStateBackend("/var/test"));
        // 设置是否清除检查点，表示cancel时是否需要保留当前的checkpoint，默认checkpointh会在作业被cancel时被删除
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION): false,当作业被取消时，保留外部的checkpoint
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION :true,当作业被取消时，删除外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //类型3：直接使用默认即可
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);  // checkpoint超时时间，如果在设置时间内还未完成，则认为失败，丢弃（默认10min）
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 同一时间有多少个checkpoint正在进行（默认1）

        //TODO == 配置重启策略
        // 1. 配置了checkpoint的情况下不做任何配置：默认是无限重启并自动恢复，可以解决小问题，但可能会隐藏BUG（不推荐使用）
        // 2. 单独配置无重启策略
         env.setRestartStrategy(RestartStrategies.noRestart());
         // 3. 固定重启策略（常用）
        // 如果job失败，就重启三次，每次间隔5s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,   // 最多重启3次
                Time.of(5, TimeUnit.SECONDS)));  // 每次重启时间间隔
        // 4. 失败率重启（一般使用）
        // 如果5min内job失败不超过三次，自动重启，每次间隔5s,(如果5min内程序失败超过三次，则程序退出)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,    // 每个测量阶段内最大失败次数
                Time.of(5,TimeUnit.MINUTES),  // 失败率测量的时间间隔
                Time.of(5,TimeUnit.SECONDS)  // 两次连续重启的时间间隔
        ));


        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost",9999);

    }
}


class MySourceFunction extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private ListState<Long> offsetState = null;  //存放offset
    private long offSet = 0L;     // offset值

    //该方法会定期执行，将state从内存转入checkpoint中
    // 持久化
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();     // 清理内容数据并存入checkpoint磁盘目录中
        offsetState.add(offSet);
    }

    // 初始化listState
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("offsetState", Long.class);
        offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);
    }

    //使用state
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Iterator<Long> iterator = offsetState.get().iterator();
        if (iterator.hasNext()){
            offSet = iterator.next();
        }
        offSet+=1;
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        ctx.collect("subTaskId"+indexOfThisSubtask+",当前offset值为"+offSet);
    }

    @Override
    public void cancel() {

    }
}