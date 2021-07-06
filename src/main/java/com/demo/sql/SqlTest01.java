package com.demo.sql;

import com.demo.bean.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTest01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        DataStreamSource<Order> orderDataStreamSourceA = streamExecutionEnvironment.fromCollection(
                Arrays.asList(
                        new Order("001", "beer", 3),
                        new Order("002", "diaper", 4),
                        new Order("003", "rubber", 2)
                )
        );
        DataStreamSource<Order> orderDataStreamSourceB = streamExecutionEnvironment.fromCollection(
                Arrays.asList(
                        new Order("002", "beer", 2),
                        new Order("002", "diaper", 2),
                        new Order("004", "rubber", 4)
                )
        );

        //DataStream转换为Table对象
        Table orderTableA = tableEnvironment.fromDataStream(orderDataStreamSourceA,$("userId"),$("product"),$("amount"));
       //DataStream转换为table表
        tableEnvironment.createTemporaryView("orderTableB",orderDataStreamSourceB,$("userId"),$("product"),$("amount"));


        Table table = tableEnvironment.sqlQuery("       " +
                " select * from "+orderTableA+"\n" +   // 从orderTableA对象查
                "            union all\n" +
                "        select * from orderTableB ");

        //Table转为DataStream
        tableEnvironment.toRetractStream(table, Order.class).print();

//        table.printSchema();
        streamExecutionEnvironment.execute();


    }
}
