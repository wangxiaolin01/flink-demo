package com.demo.sinck;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Locale;

public class RedisSinkTest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        streamExecutionEnvironment.setParallelism(1);

        DataStreamSource<String> localhost = streamExecutionEnvironment.socketTextStream("localhost", 9999);

        localhost.addSink(new RedisSink<>(new FlinkJedisPoolConfig.Builder().build(),
                new MyRedisSink()));
    }
}


class MyRedisSink implements RedisMapper<String>{

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET);
    }

    @Override
    public String getKeyFromData(String data) {
        return data.toLowerCase(Locale.ROOT);

    }

    @Override
    public String getValueFromData(String data) {
        return null;
    }
}
