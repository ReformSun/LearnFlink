package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

public class TestMain3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);

//        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("input-topic","monitorBlocklyQueueKey6");
        properties.setProperty("bootstrap.servers","172.31.24.30:9092");
        properties.setProperty("group.id","serverCollector");


        Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(properties.getProperty("input-topic"));

        jsonTableSourceBuilder.withKafkaProperties(properties);
        // set Table schema
        TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();

//        jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a",Types.STRING).field("b",Types.INT).field("rtime",Types.SQL_TIMESTAMP).build()).withRowtimeAttribute("rtime",new ExistingField("rtime"),new BoundedOutOfOrderTimestamps(30000L));
        jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a",Types.STRING).field("b",Types.INT).field("_sysTime",Types.SQL_TIMESTAMP).build()).withProctimeAttribute("_sysTime");

        KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();


        tableEnv.registerTableSource("kafkasource", kafkaTableSource);

        Table sqlResult = tableEnv.sqlQuery("SELECT a,count(b) AS DDKK FROM kafkasource GROUP BY TUMBLE(_sysTime, INTERVAL '10' SECOND),a");
        String insertSql="INSERT INTO testflink (a,num) VALUES (?,?)";
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("00org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
                .setUsername("apm").setPassword("apm")                .setQuery(insertSql)

                .setParameterTypes(STRING_TYPE_INFO,LONG_TYPE_INFO)
                .setBatchSize(1)
                .build();

//        CsvTableSink csvTableSink = new CsvTableSink("file:///E:\\Asunjihua\\idea\\LearnFlink\\src\\main\\resources\\aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);
//        csvTableSink.configure(new String[]{"sum"},new TypeInformation[]{Types.INT});
        //将数据写出去
//        sqlResult.printSchema();
        sqlResult.writeToSink(sink);
        //执行
        sEnv.execute();
    }
}
