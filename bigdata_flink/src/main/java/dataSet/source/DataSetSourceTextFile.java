package dataSet.source;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Demo class
 * DataSet —— Source —— readTextFile 从文件或者文件夹创建dataset
 * @author GUO_ZH
 * @date 2021/7/13
 */
public class DataSetSourceTextFile {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        readTextFile(env);
    }

    private static void readTextFile(ExecutionEnvironment env) throws Exception {

        String input = "file:///F:/flink/test.txt";
        env.readTextFile(input).print();
        System.out.println("---------");
        String input1 = "file:///F:/flink";
        env.readTextFile(input1).print();
    }


}
