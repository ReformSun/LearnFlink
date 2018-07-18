package standardTemplate;

import filter.TestFilter;
import filter.TestFilter2;
import flatMap_1.SunFlatMapFunction;
import flatMap_1.SunFlatMapFunction2;
import flatMap_1.SunFlatMapFunctionArgModel;
import keyby.KeySelectorIMP;
import model.SunLine;
import model.SunWordWithKey;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import reduce.TestReduce2;
import socket_1.SunSocketTextStreamFunction;
import socket_1.SunSocketTextStreamFunctionArgModel;

import java.util.ArrayList;

public class MainTemplate2 {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SunSocketTextStreamFunctionArgModel sunSocketTextStreamFunctionArgModel = new SunSocketTextStreamFunctionArgModel();
        SunSocketTextStreamFunction sunSocketTextStreamFunction = new SunSocketTextStreamFunction(sunSocketTextStreamFunctionArgModel);
        DataStreamSource<String> dataStreamSource = env.addSource(sunSocketTextStreamFunction);
        dataStreamSource.setParallelism(1);

        SunFlatMapFunction2 sunFlatMapFunction = new SunFlatMapFunction2("A",new ArrayList<String>(){{
            add("a");
            add("b");
        }});

        SingleOutputStreamOperator<SunLine> dataStream =  dataStreamSource.flatMap(sunFlatMapFunction);
        dataStream.setParallelism(1);

        SingleOutputStreamOperator<SunLine> dataStream1 =  dataStream.filter(new TestFilter2()).
                keyBy("key").timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new TestReduce2());
        dataStream1.setParallelism(1);

        SingleOutputStreamOperator<SunLine> dataStream2 =  dataStream.filter(new TestFilter2()).
                keyBy("key").timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new TestReduce2());
        dataStream2.setParallelism(1);


        SingleOutputStreamOperator<SunLine> dataStream3 =  dataStream.filter(new TestFilter2()).
                keyBy(new KeySelectorIMP()).timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new TestReduce2());
        dataStream2.setParallelism(1);

        dataStream1.print().setParallelism(1);
        dataStream2.print().setParallelism(1);

        try {
            env.execute("Socket Window WordSplit");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
