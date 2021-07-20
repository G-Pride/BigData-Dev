package dataSet.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

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
        List<Tuple2<String,Integer >> info1 = new ArrayList<Tuple2<String, Integer>>();
        info1.add(new Tuple2("messi",1));
        info1.add(new Tuple2("C罗",2));
        info1.add(new Tuple2("dybala",10));
        info1.add(new Tuple2("inesta",8));
        DataSet<Tuple2<String, Integer>> toBroadcast = env.fromCollection(info1);

        DataSet<String> data = env.fromElements("messi","J罗", "dybala","inesta");

        data.map(new RichMapFunction<String, String>() {

            HashMap map = new HashMap();

            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. Access the broadcast DataSet as a Collection
                Collection<Tuple2<String, Integer>> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                for (Tuple2<String, Integer> bc : broadcastSet){
                    map.put(bc.f0,bc.f1);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value + "的号码是：" + map.get(value);
            }
        }).withBroadcastSet(toBroadcast, "broadcastSetName").print();
        // 2. Broadcast the DataSet
    }
}
