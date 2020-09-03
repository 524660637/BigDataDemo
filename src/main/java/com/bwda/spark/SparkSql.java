package com.bwda.spark;
/*
 * @Description:
 * @Date: 19-4-11 下午2:41
 * @Author: hahaha
 * @Copyright: 2019 www.bwda.net Inc. All rights reserved.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparkSql {


    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://192.168.2.29:3306";  //hostname: mysql所在的主机名或ip地址
    private static String db = "dataSecurity";  //dbname: 数据库名
    private static String user = "root"; //username: 数据库用户名
    private static String pwd = "bwda@123"; //password: 数据库密码；若无密码，则为""
    private static String tablename = "t_sys_user";
    private static JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Test", new SparkConf());
//          private static    JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("Test").setMaster("spark://192.168.2.37:7077"));

    private static SparkSession spark = SparkSession.builder().appName("Test").getOrCreate();

    public static class Person  implements Serializable {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
        public String toString() {
            return name + ": " + age;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public int getAge() {
            return age;
        }
        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        //加载三张表
        Dataset<Row> user =  readTableBySparkOption("t_sys_user");
        Dataset<Row> userGroup =  readTableBySparkOption("t_sys_user_group");
        Dataset<Row> group =  readTableBySparkOption("t_sys_group");
        user.registerTempTable("user");
        userGroup.registerTempTable("userGroup");
        group.registerTempTable("group");
        Dataset<Row> jointResult = spark.sql("select * from user t1 \n" +
                "inner join userGroup t2 \n" +
                "on t1.userid=t2.userid \n" +
                "inner join group t3\n" +
                "on t3.groupid=t2.groupid\n" +
                "where t2.groupid=358");
//        select * from t_sys_user t1
//        inner join t_sys_user_group t2
//        on t1.userid=t2.userid
//        inner join t_sys_group t3
//        on t3.groupid=t2.groupid
//        where t2.groupid=358
//        mysql原语句
        jointResult.show();
//         readTableBySparkOption("t_sys_user");
        javaSparkContext.stop();
    }
    public static void testRdd(){
        System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Test",new SparkConf());
//        JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("Test").setMaster("spark://192.168.2.37:7077"));
//        conf.set("spark.driver.host", "192.168.2.37");
        SparkSession spark = SparkSession.builder().appName("Test").getOrCreate();
        //转化操作
        JavaRDD<String> input = javaSparkContext.parallelize(Arrays.asList("abc,1","abc,2","abc,3","abc,1","abc,1", "test,2"));
        JavaPairRDD<String,Integer > inputPair = input.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0],new Integer(s.split(",")[1]));
            }
        });
//        System.err.println(inputPair.collect());
        JavaRDD<Person> persons = input.map(s -> s.split(",")).map(s -> new Person(s[0], Integer.parseInt(s[1])));
//        JavaRDD<Person> personChild = persons.map(new Function<Person, Person>() {
//            @Override
//            public Person call(Person person) throws Exception {
//                if(person.getAge()<6){
//                    person.setName("child");
//                }
//                return new Person(person.getName(),person.getAge());
//            }
//        });

        Dataset<Row> df = spark.createDataFrame(persons, Person.class);

        /*
        +---+----+
        |age|name|
        +---+----+
        | 1| abc|
        | 2|test|
        +---+----+
         */
//        df.show();

        /*
         root
          |-- age: integer (nullable = false)
          |-- name: string (nullable = true)
         */
//        df.printSchema();

        SQLContext sql = new SQLContext(spark);
        sql.registerDataFrameAsTable(df, "person");

        /*
        +---+----+
        |age|name|
        +---+----+
        | 2|test|
        +---+----+
         */
        sql.sql("SELECT * FROM person WHERE age>1").show();

        javaSparkContext.close();
    }

    public static Dataset<Row> readTableBySparkOption (String tablename){
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver",driver)
                .option("url", url)
                .option("dbtable", db + "." + tablename)
                .option("user", user)
                .option("password", pwd)
                .load();
        return jdbcDF;
    }

    public static void readTableBySparkOptions(String tablename) {
        Map<String, String> map = new HashMap<String, String>() {{
            put("driver", driver);
            put("url", url);
            put("dbtable", db + "." + tablename);
            put("user", user);
            put("password", pwd);
        }};
        Dataset<Row> jdbcDF = spark.read().format("jdbc").options(map).load();
        jdbcDF.show();
    }

    public static void readTableBySparkProperty( ) {
        Properties connectedProperties = new Properties();
        connectedProperties.put("user", user);
        connectedProperties.put("password", pwd);
        connectedProperties.put("customSchema", "id STRING, name STRING");  //用来具体化表结构，去掉不影响程序执行
        Dataset<Row> jdbcDF2 = spark.read() .jdbc(url, db + "." + tablename, connectedProperties);
        jdbcDF2.show();
    }


}
