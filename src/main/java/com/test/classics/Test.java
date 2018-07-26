package com.test.classics;

import com.test.flatMap_1.SunFlatMapFunction;
import com.test.flatMap_1.SunFunctionStates1;
import model.SunWordWithCount;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Test {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);

//        DataStream<String> dataStreamSource = env.readTextFile(".\\src\\main\\resources\\test.txt");

        DataStream<String> dataStreamSource = env.socketTextStream("localhost", 9000, "\n");
        SunFunctionStates1 sunFunctionStates1 = new SunFunctionStates1();
        SunFlatMapFunction sunFlatMapFunction = new SunFlatMapFunction();

        DataStream<SunWordWithCount> dataStream =  dataStreamSource.flatMap(sunFlatMapFunction).keyBy("word").timeWindow(Time.seconds(6), Time.seconds(2))
                .reduce(new ReduceFunction<SunWordWithCount>() {
                    @Override
                    public SunWordWithCount reduce(SunWordWithCount a, SunWordWithCount b) {
                        return new SunWordWithCount(a.word,a.count + b.count);
                    }
                });

        dataStream.print().setParallelism(1);

        try {
            env.execute("Socket Window WordSplit");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
