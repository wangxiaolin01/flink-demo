package com.mmain;

import com.hand.bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class GussionTest  implements SourceFunction<WaterSensor> {
    boolean running = true;
        Random random = new Random();
        HashMap<Integer, Double> sensorTemp = new HashMap<>();

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        for (int i = 1;i<11;i++){
            sensorTemp.put(i,60+random.nextGaussian());
        }

        while (running){
            for (Integer waterKey:sensorTemp.keySet()){
                Double newTemp = sensorTemp.get(waterKey)+random.nextGaussian();
                sensorTemp.put(waterKey,newTemp);
                ctx.collect(new WaterSensor(waterKey,newTemp,System.currentTimeMillis()));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
