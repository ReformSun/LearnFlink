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

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;

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
        jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a",Types.STRING).field("b",Types.INT).field("rtime",Types.SQL_TIMESTAMP).build()).withProctimeAttribute("rtime");

        KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();


        tableEnv.registerTableSource("kafkasource", kafkaTableSource);

//        Table table = tableEnv.scan("kafkasource");
//        Table sqlResult = table.window(Tumble.over("1.minutes").on(
//"rtime").as("a")).groupBy("a").select("b.sum as num");


//        Table sqlResult = table.window(Tumble.over("10.rows").on("rtime").as("a")).groupBy("a").select("b.sum");

//        StreamQueryConfig qConfig = new StreamQueryConfig();
//



        Table sqlResult = tableEnv.sqlQuery("SELECT count(a) FROM kafkasource");
//        String insertSql="INSERT INTO testflink (num) VALUES (?)";
//        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
//                .setDrivername("org.postgresql.Driver")
//                .setDBUrl("jdbc:postgresql://10.4.247.20:5432/apm_test")
//                .setUsername("apm").setPassword("apm")
//                .setQuery(insertSql)
//                .setParameterTypes(INT_TYPE_INFO)
//                .setBatchSize(1)
//                .build();

        CsvTableSink csvTableSink = new CsvTableSink("file:///E:\\Asunjihua\\idea\\LearnFlink\\src\\main\\resources\\aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);
//        csvTableSink.configure(new String[]{"sum"},new TypeInformation[]{Types.INT});
        //将数据写出去
//        sqlResult.printSchema();
        sqlResult.writeToSink(csvTableSink);
        //执行
        sEnv.execute();
    }
}
