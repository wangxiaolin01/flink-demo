package com.demo.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

public class ParalizeSource extends RichParallelSourceFunction<String> {
    private Boolean flag = true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Random random = new Random();
        while (flag){
          String UUID = java.util.UUID.randomUUID().toString();
          int userId = random.nextInt(5);
          double money = random.nextDouble()*100;
        }
    }

    @Override
    public void cancel() {

    }
}
