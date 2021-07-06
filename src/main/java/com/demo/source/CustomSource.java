package com.demo.source;

import com.demo.bean.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

public class CustomSource implements SourceFunction<SensorReading> {
    private boolean flag = true;
    Random random = new Random();
    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        if (flag){
            String uuid = UUID.randomUUID().toString();
            Double temperature = random.nextDouble()*30;
            Long timestamp = System.currentTimeMillis() - random.nextInt(5)*1000;

            ctx.collect(new SensorReading(uuid,temperature,timestamp));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
