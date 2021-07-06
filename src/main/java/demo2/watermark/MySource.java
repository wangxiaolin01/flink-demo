package demo2.watermark;

import demo2.watermark.bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class MySource extends RichParallelSourceFunction<WaterSensor> {
    private boolean flag = true;
    private long  id = 100101;
    private double[] tempwe = {37.0,36.9,35.6,35.9,36.2,37.3,17.1,37.2,37.5,38.0,38.3,38.1,38.2};
    Random random = new Random();
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (flag){
            Thread.sleep(1000);
            long currentTimestamp = System.currentTimeMillis();
            int index = random.nextInt(tempwe.length);
//            System.out.println(index);
            double temp = tempwe[index];
            ctx.collect(new WaterSensor(id+"",currentTimestamp,temp));
            id++;
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
