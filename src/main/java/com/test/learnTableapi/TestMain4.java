package com.test.learnTableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;

import java.util.Properties;

public class TestMain4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(sEnv);

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("input-topic","monitorBlocklyQueueKey6");
        properties.setProperty("bootstrap.servers","172.31.24.30:9092");
        properties.setProperty("group.id","serverCollector");


        Kafka010JsonTableSource.Builder jsonTableSourceBuilder = Kafka010JsonTableSource.builder().forTopic(properties.getProperty("input-topic"));

        jsonTableSourceBuilder.withKafkaProperties(properties);
        // set Table schema
        TableSchemaBuilder tableSchemaBuilder= TableSchema.builder();

//        jsonTableSourceBuilder.withSchema(tableSchemaBuilder.field("a",Types.STRING).field("b",Types.INT).field("rtime",Types.SQL_TIMESTAMP).build()).withRowtimeAttribute("rtime",new ExistingField("rtime"),new BoundedOutOfOrderTimestamps(30000L));
        jsonTableSourceBuilder.withSchema(tableSchemaBuilder
                .field("_line", Types.STRING)
                .field("cc1",Types.STRING)
                .field("cc2",Types.STRING)
                .field("cc3",Types.STRING)
                .field("_sysTime",Types.SQL_TIMESTAMP)
                .build())
                .withRowtimeAttribute("_sysTime", new ExistingField("_sysTime"),new BoundedOutOfOrderTimestamps(1000L));

        KafkaTableSource kafkaTableSource=jsonTableSourceBuilder.build();
        tableEnv.registerTableSource("kafkasource", kafkaTableSource);
        Table sqlResult = tableEnv.sqlQuery("SELECT _sysTime,count(_line),cc1,cc2,cc3 FROM kafkasource GROUP BY TUMBLE(_sysTime,INTERVAL '10' SECOND),cc1,cc3,cc2,_sysTime");

        CsvTableSink csvTableSink = new CsvTableSink("file:///E:\\Asunjihua\\idea\\LearnFlink\\src\\main\\resources\\aaaaaa.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);
        sqlResult.writeToSink(csvTableSink);
        //执行
        sEnv.execute();
    }
}
