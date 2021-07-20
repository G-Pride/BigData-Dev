package dataSet.demo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Demo class
 * 计数器
 * @author GUO_ZH
 * @date 2021/7/20
 */
public class JavaCounterApp {

    public static void main(String[] args) throws Exception {
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hadoop","spark","flink","pyspark","storm");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            LongCounter counter = new LongCounter();

            //注册计数器
            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("javaCount",counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePath = "file:///F:/flink/test_sink/";
        result.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);

        JobExecutionResult jobExecutionResult = env.execute("JavaCounterApp");
        //获取计数器
        long num = jobExecutionResult.getAccumulatorResult("javaCount");
        System.out.println("计数器结果为:"+num);
    }

}
