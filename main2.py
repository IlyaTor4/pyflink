from pyflink.table import EnvironmentSettings, TableEnvironment

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


table_env.execute_sql("""
    CREATE TABLE kafka_source (
        user_fkey STRING, 
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
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'flink-group-ilya',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false'
    )
""")

table_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS user_stats (
        user_fkey STRING, 
        risk_amount INT,
        PRIMARY KEY (user_fkey) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/flinkdb',
        'table-name' = 'user_stats',
        'driver' = 'org.postgresql.Driver',
        'username' = 'postgres',
        'password' = 'postgres',
        'sink.buffer-flush.max-rows' = '1000',
        'sink.buffer-flush.interval' = '10000',
        'sink.max-retries' = '5'
    )
""").wait()


table_env.execute_sql("""
INSERT INTO user_stats
SELECT user_fkey, risk_amount 
FROM kafka_source
""").wait()
