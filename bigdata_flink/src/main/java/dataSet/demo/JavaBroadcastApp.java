package dataSet.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Demo class
 *
 * @author GUO_ZH
 * @date 2021/7/20
 */
public class JavaBroadcastApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1. The DataSet to be broadcast
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");

        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. Access the broadcast DataSet as a Collection
                Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                System.out.println("broadcastSet = [" + broadcastSet + "]");
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).withBroadcastSet(toBroadcast, "broadcastSetName").print();
        // 2. Broadcast the DataSet
    }
}
