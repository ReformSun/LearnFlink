package standardTemplate;

import filter.TestFilter;
import flatMap_1.SunFlatMapFunction;
import flatMap_1.SunFlatMapFunctionArgModel;
import map.Test;
import model.SunWordWithKey;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import reduce.TestReduce;
import reduce.TestReduce2;
import reduce.TestReduce3;
import service.socket.TestThread;
import socket_1.SunSocketTextStreamFunction;
import socket_1.SunSocketTextStreamFunctionArgModel;

public class MainTemplate {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        SunSocketTextStreamFunctionArgModel sunSocketTextStreamFunctionArgModel = new SunSocketTextStreamFunctionArgModel();
//        SunSocketTextStreamFunction sunSocketTextStreamFunction = new SunSocketTextStreamFunction(sunSocketTextStreamFunctionArgModel);
//        DataStreamSource<String> dataStreamSource = env.addSource(sunSocketTextStreamFunction);

        DataStream<String> dataStreamSource = env.socketTextStream("localhost",9000, "\n");

        SunFlatMapFunctionArgModel sunFlatMapFunctionArgModel = new SunFlatMapFunctionArgModel();
        SunFlatMapFunction sunFlatMapFunction = new SunFlatMapFunction(sunFlatMapFunctionArgModel);
//        测试map

//        测试flatMap
        DataStream<SunWordWithKey> dataStream =  dataStreamSource.flatMap(sunFlatMapFunction);


        DataStream<SunWordWithKey> dataStream1 =  dataStream.keyBy("value").timeWindow(Time.seconds(5),Time.seconds(1)).reduce(new TestReduce3());


        dataStream1.print().setParallelism(1);


//        NetworkEnvironment



//        DataStream<S> dataStream2 =  dataStreamSource.flatMap(sunFlatMapFunction).keyBy("word").timeWindow(Time.seconds(5),Time.seconds(1)).reduce(new TestReduce2());


        try {
            env.execute("Socket Window WordSplit");


        } catch (Exception e) {
            e.printStackTrace();
        }

//        TaskManager

//
//
//        StreamGraph streamGraph = env.getStreamGraph();
////        streamGraph.getJobGraph().addJar();
//
//        DataStream<String> dataStream2 = dataStream.map(new Test());
//        dataStream2.print().setParallelism(1);
//        env.execute("Socket Window WordSplit");


        Thread.currentThread().getStackTrace()[1].getLineNumber();





    }
}
