package wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.omg.CORBA.Environment;

/**
 * Demo class
 *
 * @author GUO_ZH
 * @date 2021/7/10
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        String input = "file:///F:/flink/test.txt";
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //read data
        DataSource<String> dataSource =  env.readTextFile(input);
        //transform
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] values = value.toLowerCase().split(" ");
                for (String val:values){
                    out.collect(new Tuple2<String, Integer>(val,1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
