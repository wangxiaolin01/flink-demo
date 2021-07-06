package com.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Random;

public class DoubleEventBigSource implements SourceFunction<Tuple2<String,BigDecimal>> {
    private boolean flag = true;
    private String[] categorys = {"女装","男装","图书","家电","洗护","美妆","运动","游戏","户外","家具","乐器","办公"};
    private Random random = new Random();

    @Override
    public void run(SourceContext<Tuple2<String, BigDecimal>> ctx) throws Exception {
        while (flag){
            int index = random.nextInt(categorys.length);
            String category = categorys[index];
            double price = random.nextFloat() * 100;
            DecimalFormat df1 = new DecimalFormat("0.00");

            BigDecimal bigDecimal = BigDecimal.valueOf(price);
            ctx.collect(new Tuple2<>(category,bigDecimal));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
