package standardTemplate;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main1 {
    public static void main(String[] args) {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStreamSource = env.socketTextStream("localhost",9000, "\n");






        try {
            env.execute("Socket Window WordSplit");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
