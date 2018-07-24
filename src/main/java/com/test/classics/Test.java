package com.test.classics;

import com.test.flatMap_1.SunFunctionStates1;
import model.SunWordWithCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);

        DataStream<String> dataStreamSource = env.readTextFile("E:\\Asunjihua\\idea\\LearnFlink\\src\\main\\resources\\test.txt");
        SunFunctionStates1 sunFunctionStates1 = new SunFunctionStates1();
        DataStream<SunWordWithCount> dataStream =  dataStreamSource.keyBy("word").flatMap(sunFunctionStates1);

        dataStream.print().setParallelism(1);

        try {
            env.execute("Socket Window WordSplit");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
