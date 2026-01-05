package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

import java.util.Properties;

/**
 * Flink Java 示例代码
 * 演示Flink DataStream API和Table API的Java实现
 */
public class FlinkJavaExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        
        // 启用checkpoint
        env.enableCheckpointing(60000);
        
        // 创建Table环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        // Kafka配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "flink-java-group");
        
        // 创建Kafka Source
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "input-topic",
                new SimpleStringSchema(),
                kafkaProps
        );
        
        // 添加Source
        DataStream<String> stream = env.addSource(kafkaConsumer);
        
        // 数据处理
        DataStream<String> processedStream = stream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value.toUpperCase();
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.length() > 5;
                    }
                })
                .keyBy(value -> value.substring(0, 1))
                .timeWindow(Time.minutes(5))
                .reduce((v1, v2) -> v1 + "," + v2);
        
        // 创建Kafka Sink
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                kafkaProps
        );
        
        // 添加Sink
        processedStream.addSink(kafkaProducer);
        
        // 执行
        env.execute("Flink Java Example");
    }
    
    /**
     * 使用Table API的示例
     */
    public static void tableApiExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 执行SQL创建表
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id STRING," +
            "  user_id STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'orders'," +
            "  'properties.bootstrap.servers' = 'kafka:9092'," +
            "  'format' = 'json'" +
            ")"
        );
        
        // 执行窗口聚合
        tableEnv.executeSql(
            "SELECT " +
            "  TUMBLE_START(order_time, INTERVAL '1' MINUTE) AS window_start," +
            "  user_id," +
            "  SUM(amount) AS total_amount " +
            "FROM orders " +
            "GROUP BY TUMBLE(order_time, INTERVAL '1' MINUTE), user_id"
        );
    }
}
