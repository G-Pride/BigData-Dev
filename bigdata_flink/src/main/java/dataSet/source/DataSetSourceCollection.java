package dataSet.source;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Demo class
 * DataSet —— Source —— Collection 从集合创建dataset
 * @author GUO_ZH
 * @date 2021/7/13
 */
public class DataSetSourceCollection {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        fromCollection(env);
    }

    private static void fromCollection(ExecutionEnvironment env) throws Exception {

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}
