# -*- coding: utf-8 -*-
"""
Flink/PyFlink 示例代码
演示Flink的DataStream API和Table API
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types


def datastream_example():
    """DataStream API 示例"""
    # 创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    # 启用checkpoint
    env.enable_checkpointing(60000)  # 60秒
    
    # 从集合创建数据流
    data_stream = env.from_collection(
        collection=[(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)],
        type_info=Types.TUPLE([Types.INT(), Types.STRING(), Types.INT()])
    )
    
    # 数据转换
    result_stream = data_stream \
        .map(lambda x: (x[0], x[1].upper(), x[2] * 2)) \
        .filter(lambda x: x[2] > 50) \
        .key_by(lambda x: x[0])
    
    # 打印结果
    result_stream.print()
    
    # 执行
    env.execute("DataStream Example")


def kafka_source_sink_example():
    """Kafka Source/Sink 示例"""
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Kafka消费者配置
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-consumer-group'
    }
    
    # 创建Kafka Source
    kafka_consumer = FlinkKafkaConsumer(
        topics='input-topic',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # 添加Source
    stream = env.add_source(kafka_consumer)
    
    # 处理数据
    processed = stream.map(lambda x: x.upper())
    
    # 创建Kafka Sink
    kafka_producer = FlinkKafkaProducer(
        topic='output-topic',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    
    # 添加Sink
    processed.add_sink(kafka_producer)
    
    env.execute("Kafka Source Sink Example")


def table_api_example():
    """Table API 示例"""
    # 创建Table环境
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    
    # 设置配置
    t_env.get_config().set("parallelism.default", "4")
    
    # 创建Source表 (Kafka)
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            user_id INT,
            user_name STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-table-group',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)
    
    # 创建Sink表
    t_env.execute_sql("""
        CREATE TABLE result_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            user_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://mysql:3306/analytics',
            'table-name' = 'user_stats',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'username' = 'root',
            'password' = 'password'
        )
    """)
    
    # 执行窗口聚合查询
    t_env.execute_sql("""
        INSERT INTO result_sink
        SELECT 
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            COUNT(DISTINCT user_id) AS user_count
        FROM kafka_source
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)


def flink_sql_example():
    """Flink SQL 示例"""
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    
    # 使用SQL查询
    result = t_env.sql_query("""
        SELECT 
            user_id,
            COUNT(*) as cnt,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY user_id
    """)
    
    # 创建临时视图
    t_env.create_temporary_view("temp_orders", result)
    
    # 执行SQL
    t_env.execute_sql("""
        CREATE TABLE orders_with_watermark (
            order_id STRING,
            user_id STRING,
            amount DECIMAL(10, 2),
            order_time TIMESTAMP(3),
            WATERMARK FOR order_time AS order_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'format' = 'json'
        )
    """)


def window_example():
    """窗口操作示例"""
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    
    # 滚动窗口 TUMBLE
    t_env.execute_sql("""
        SELECT 
            TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
            user_id,
            COUNT(*) as event_count
        FROM events
        GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), user_id
    """)
    
    # 滑动窗口 HOP
    t_env.execute_sql("""
        SELECT 
            HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_start,
            user_id,
            AVG(amount) as avg_amount
        FROM orders
        GROUP BY HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE), user_id
    """)
    
    # 会话窗口 SESSION
    t_env.execute_sql("""
        SELECT 
            SESSION_START(event_time, INTERVAL '10' MINUTE) as session_start,
            user_id,
            COUNT(*) as click_count
        FROM clicks
        GROUP BY SESSION(event_time, INTERVAL '10' MINUTE), user_id
    """)


def main():
    print("Running Flink Examples...")
    
    # DataStream示例
    datastream_example()
    
    # Table API示例
    table_api_example()
    
    print("Flink Examples completed.")


if __name__ == "__main__":
    main()
