package com.test.learnWindows;

import model.SunWordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class TestMain1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(60000);
//        env.setStateBackend(new RocksDBStateBackend("file:////Users/apple/Desktop/rockdata"));

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("172.31.35.58", 9000, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<SunWordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, SunWordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<SunWordWithCount> out) {

                        for (String word : value.split("\\s")) {
                            out.collect(new SunWordWithCount(word, 1L));
                        }
                    }
                }).keyBy("word")
                .timeWindow(Time.seconds(60), Time.seconds(20))
                .reduce(new ReduceFunction<SunWordWithCount>() {
                    @Override
                    public SunWordWithCount reduce(SunWordWithCount a, SunWordWithCount b) {
                        return new SunWordWithCount(a.word,a.count + b.count);
                    }
                });




        windowCounts.print().setParallelism(1);

//        windowCounts.writeAsText(".\\src\\main\\resources\\test1.txt", FileSystem.WriteMode.NO_OVERWRITE);

        env.execute("Socket Window WordCount");
    }


}
