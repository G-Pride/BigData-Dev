package dataSet.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

class DataSetSourceTextFile {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         mapFuntion(env);

         //filterFunction(env);

        //mapPartitionFunction(env);
    }

    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        dataSource.mapPartition(new MapPartitionFunction<Integer, Object>() {
            @Override
            public void mapPartition(Iterable<Integer> values, Collector<Object> out) throws Exception {
                for (Integer val:values){
                    out.collect(val+1);
                }
                System.out.println("mapPartition: "+out);
            }
        }).print();
    }

    private static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);

        dataSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value+1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 5;
            }
        }).print();
    }

    private static void mapFuntion(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);

        dataSource.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("MapFunction:"+(value + 1));
                return value + 1;
            }
        }).print();
    }
}
