package dataSet.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Demo class
 *
 * @author GUO_ZH
 * @date 2021/7/20
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> info = new ArrayList<Integer>();
        for(int i=1; i<=10; i++) {
            info.add(i);
        }

        String filePath = "file:///F:/flink/test_sink.txt";
        DataSource<Integer> data = env.fromCollection(info);
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

        env.execute("JavaDataSetSinkApp");
    }

}
