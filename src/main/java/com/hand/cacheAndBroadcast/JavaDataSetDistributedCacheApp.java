package com.hand.cacheAndBroadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDistributedCacheApp {
    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1. 注册一个本地/hdfs文件
        env.registerCachedFile("","test");
        DataSource<String> data = env.fromElements("hadoop", "spark", "flink");

        data.map(new RichMapFunction<String, String>() {
            List<String> list = new ArrayList<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("");
                List<String> strings = FileUtils.readLines(file);
                for (String line: strings) {
                    list.add(line);
                }

            }

            @Override
            public String map(String s) throws Exception {
                return null;
            }
        });

































    }
}
