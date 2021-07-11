package wordCount.SpecifyingWordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Demo class
 *  基于 wordCount.StreamWordCount 进行 Specifying 改造
 * @author GUO_ZH
 * @date 2021/7/10
 */
class SpecifyingWordCount {

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

        DataStreamSource<String> dataStreamSource =  env.socketTextStream("127.0.0.1",port);

        dataStreamSource.flatMap(new MyFlatMapFunction())
                .keyBy(new KeySelector<WordCount, Object>() {
                    @Override
                    public Object getKey(WordCount wordCount) throws Exception {
                        return wordCount.getWord();
                    }
                })//指定key之key选择器函数
                .timeWindow(Time.seconds(1))
                .sum("count")
                .print();

        env.execute("SpecifyingWordCount");
    }

    /**
     * 自定义转换函数
     */
    public static class MyFlatMapFunction implements FlatMapFunction<String, WordCount>{
        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            String[] values = value.split(" ");
            for (String val : values){
                out.collect(new WordCount(val,1));
            }
        }
    }

    /**
     * 自定义字段表达式
     */
    public static class WordCount{
        /*
            注意：如果使用 private，必须生成对应的get、set方法，否则报错：cannot be used as a POJO type because not all fields are valid POJO fields
            如果使用public，则不需要生成对应的get、set方法。
        */
        private String word;
        private int count;

        //不可忽略，否则报错：missing a default constructor so it cannot be used as a POJO type and must be processed as GenericType
        public WordCount() {
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }

}
