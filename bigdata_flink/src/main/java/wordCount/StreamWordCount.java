package wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.omg.Dynamic.Parameter;

/**
 * Demo class
 *  实时统计来自socket的数据
 * @author GUO_ZH
 * @date 2021/7/10
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        int port = 0;
        //通过 --port 8081
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设定，默认使用 8081");
            port = 8081;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<String> dataStreamSource =  env.socketTextStream("127.0.0.1",8081);//端口写死，不够灵活
        DataStreamSource<String> dataStreamSource =  env.socketTextStream("127.0.0.1",port);

        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] values = value.split(" ");
                for (String val : values){
                    out.collect(new Tuple2<String,Integer>(val,1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(1)).sum(1).print();

        env.execute("StreamWordCount");
    }

}
