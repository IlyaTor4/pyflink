from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
tenv = StreamTableEnvironment.create(env, settings)

tenv.execute_sql("""
    CREATE TABLE kafka_source (
        user_fkey VARCHAR, 
        type INT,
        currency_code INT,
        risk_amount INT,
        us_coeff INT,
        expected_payout_amount INT,
        selections_count INT,
        stake_category INT,
        settlement_state INT,
        settlement_stamp_millis_utc INT,
        settlement_payout_amount INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'picks',
        'properties.bootstrap.servers' = 'b-1.mskdev01.uw24ta.c4.kafka.us-east-2.amazonaws.com:9092',
        'properties.group.id' = 'flink-group-ilya',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false'
    )
""")

tenv.execute_sql("""
    CREATE TABLE print (
        user_fkey STRING,
        risk_amount INT
    ) WITH (
        'connector' = 'print'
    )
""")

source_table = tenv.from_path("kafka_source")

result_table = source_table.select(col("user_fkey"), col("risk_amount"))

# 5. emit query result to sink table
# emit a Table API result Table to a sink table:
result_table.execute_insert("print").wait()
# or emit results via SQL query:
# table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").wait()
