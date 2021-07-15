package dataSet.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * Demo class
 * DataSet —— Source —— readTextFile 从文件或者文件夹创建dataset
 * @author GUO_ZH
 * @date 2021/7/13
 */
public class DataSetSourceCsvFile {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        readCsvFile(env);
    }

    private static void readCsvFile(ExecutionEnvironment env) throws Exception {

        //csv格式为UTF-8
        String input = "file:///F:/flink/csvTest.csv";

        // read a CSV file with two fields
        DataSet<Tuple2<String,Integer>> csvInput = env.readCsvFile(input)
                .ignoreFirstLine()
                .types(String.class,Integer.class);
        //csvInput.print();
        System.out.println("-----------------");

        // read a CSV file with two fields, taking only one of them
        DataSet<Tuple1<String>> csvInput2 = env.readCsvFile(input)
                .ignoreFirstLine()
                .includeFields("10")  // take the first field
                .types(String.class);
        //csvInput2.print();
        System.out.println("-----------------");

        // read a CSV file with three fields into a POJO (People.class) with corresponding fields
        DataSet<People> csvInput3 = env.readCsvFile(input)
                .fieldDelimiter(",")//表示分隔符
                .ignoreFirstLine()//表示是否忽略第一行
                .includeFields(true,true)//表示需要展示的列
                .pojoType(People.class,"name","age");
        //csvInput3.print();

        /**
         * 读取整个目录下的文件
         */
        // create a configuration object
        Configuration parameters = new Configuration();
        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);
        // pass the configuration to the data source
        DataSet<String> logs = env.readTextFile("file:///F:/flink/")
                .withParameters(parameters);
        //logs.print();
    }


}
