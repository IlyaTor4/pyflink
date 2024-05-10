from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    tenv = StreamTableEnvironment.create(env, settings)

    src_ddl = """
        CREATE TABLE picks_data (
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
            settlement_payout_amount INT,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'picks',
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id' = 'flink_picks',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    tenv.execute_sql(src_ddl)

    tumbling_w_sql = """
            SELECT
                user_fkey,
                TUMBLE_START(proctime, INTERVAL '30' SECONDS) AS window_start,
                TUMBLE_END(proctime, INTERVAL '30' SECONDS) AS window_end,
                SUM(risk_amount) AS total_risk_amount
            FROM picks_data
            GROUP BY
                TUMBLE(proctime, INTERVAL '30' SECONDS),
                user_fkey
        """

    tumbling_w = tenv.sql_query(tumbling_w_sql)

    sink_ddl = """
            CREATE TABLE user_stats_window (
                user_fkey VARCHAR,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                total_risk_amount FLOAT
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres:5432/flinkdb',
                'table-name' = 'user_stats_window',
                'driver' = 'org.postgresql.Driver',
                'username' = 'postgres',
                'password' = 'postgres',
                'sink.buffer-flush.max-rows' = '1000',
                'sink.buffer-flush.interval' = '10000',
                'sink.max-retries' = '5'
            )
        """

    tenv.execute_sql(sink_ddl)
    tumbling_w.execute_insert('user_stats_window').wait()


if __name__ == '__main__':
    main()
