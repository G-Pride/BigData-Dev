package dataSet.transformation;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

class DataSetSourceTextFile {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         //mapFuntion(env);

         //filterFunction(env);

        //mapPartitionFunction(env);

        //firstFunction(env);

        //flatMapFunction(env);

        //distinctFunction(env);

        //joinFunction(env);

        //outerJoinFunction(env);

        crossFunction(env);
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<String>();
        info1.add("曼联");
        info1.add("曼城");


        List<String> info2 = new ArrayList<String>();
        info2.add("3");
        info2.add("1");
        info2.add("0");

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        //左外连接
        System.out.println("左外连接：");
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");

                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
        /**
         * (3,小队长,成都)
         * (1,PK哥,北京)
         * (2,J哥,上海)
         * (4,猪头呼,-)
         */

        //右外连接
        System.out.println("右外连接：");
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
        /**
         * (3,小队长,成都)
         * (1,PK哥,北京)
         * (5,-,杭州)
         * (2,J哥,上海)
         */

        //full连接
        System.out.println("full连接：");
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
        /**
         * (3,小队长,成都)
         * (1,PK哥,北京)
         * (5,-,杭州)
         * (2,J哥,上海)
         * (4,猪头呼,-)
         */

    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

       data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
           @Override
           public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

               return new Tuple3<Integer, String, String>(first.f0,second.f1,first.f1);
           }
       }).print();

    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).groupBy(0).sum(1).print();
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2(1, "Hadoop"));
        info.add(new Tuple2(1, "Spark"));
        info.add(new Tuple2(1, "Flink"));
        info.add(new Tuple2(2, "Java"));
        info.add(new Tuple2(2, "Spring Boot"));
        info.add(new Tuple2(3, "Linux"));
        info.add(new Tuple2(4, "VUE"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        //取全部的前3条
        data.first(3).print();
        /**
         * (1,Hadoop)
         * (1,Spark)
         * (1,Flink)
         */
        System.out.println("~~~~~~~~~~");

        //按第一个字段分组后，取每组的前两个
        data.groupBy(0).first(2).print();
        /**
         * (3,Linux)
         * (1,Hadoop)
         * (1,Spark)
         * (2,Java)
         * (2,Spring Boot)
         * (4,VUE)
         */
        System.out.println("~~~~~~~~~~");

        //按第一个字段分组后，按第二个字段倒序后取每组的前两个
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
        /**
         * (3,Linux)
         * (1,Spark)
         * (1,Hadoop)
         * (2,Spring Boot)
         * (2,Java)
         * (4,VUE)
         */

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
