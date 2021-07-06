package com.demo.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MysqlSink {
}

class CustomMysqlSink extends RichSinkFunction<String>{
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
