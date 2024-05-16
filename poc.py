import json
import os
from dataclasses import dataclass

from pyflink.common import Row
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, kafka
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.functions import (
    FlatMapFunction,
    MapFunction,
    ProcessFunction,
)


@dataclass
class Pick:
    user_fkey: str
    user_id: int
    risk_amount: int
    settlement_state: int
    currency_code: int
    created_stamp_millis_utc: int


@dataclass
class UserStats:
    user_id: int
    total_picks: int
    total_risk_amount: int
    won: int
    loss: int
    push: int

# TODO: FLAT_MAPP
class PickMapper(MapFunction):
    def map(self, data: str):
        fo = json.loads(data)
        return Pick(
            user_fkey=fo['user_fkey'],
            user_id=int(fo['user_fkey'].split('__')[-1]),
            risk_amount=fo['risk_amount'],
            settlement_state=fo['settlement_state'],
            currency_code=fo['currency_code'],
            created_stamp_millis_utc=fo['created_stamp_millis_utc'],
        )


class PickProcess(ProcessFunction):
    def process_element(self, pick: Pick, ctx):
        u_stats = UserStats(pick.user_id, 0, 0, 0, 0, 0)
        if pick.currency_code == 333:
            u_stats = UserStats(
                pick.user_id,
                1,
                pick.risk_amount,
                1 if pick.settlement_state == 511 else 0,
                1 if pick.settlement_state == 512 else 0,
                1 if pick.settlement_state == 513 else 0,
            )

        yield u_stats


class StatsRowMapper(MapFunction):
    def map(self, data: UserStats):
        return Row(
            data.user_id,
            json.dumps({
                'won': data.won,
                'loss': data.loss,
                'push': data.push,
                'total_coins_staked': data.total_risk_amount,
                'total_settled_picks': data.total_picks,
            }),
            data.user_id,
            data.user_id,
        )


CHECKPOINT_INTERVAL = 5000
CHECKPOINT_TIMEOUT = 60000
MIN_INTERVAL_BETWEEN_CHECKPOINTS = 2000

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(CHECKPOINT_INTERVAL)
env.get_checkpoint_config().set_min_pause_between_checkpoints(MIN_INTERVAL_BETWEEN_CHECKPOINTS)
env.get_checkpoint_config().set_checkpoint_timeout(CHECKPOINT_TIMEOUT)

kafka_source = (
    kafka.KafkaSource.builder()
    .set_bootstrap_servers('b-1.mskdev01.uw24ta.c4.kafka.us-east-2.amazonaws.com:9092')
    .set_topics('flink-picks-2')
    .set_starting_offsets(kafka.KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)


def sink_to_db(ds: DataStream) -> None:
    BATCH_INTERVAL_MS = 5000
    BATCH_SIZE = 100
    MAX_RETRIES = 5
    OUTPUT_TYPE_INFO = Types.ROW([Types.INT(), Types.STRING(), Types.INT(), Types.INT()])

    upsert_query = (
        '''
        with aggregated_stats as (SELECT user_id,
                                         json_object_agg(key, sum) AS general_stats
                                  FROM (SELECT user_id,
                                               key,
                                               sum(value::INT) AS sum
                                        FROM (SELECT user_id,
                                                     key,
                                                     value
                                              FROM (VALUES (?, ?::JSONB),
                                                           (?, (SELECT general_stats FROM user_stats WHERE user_id = ?))
                                              ) AS mytable(user_id, stats)
                                              CROSS JOIN LATERAL jsonb_each_text(stats)) AS subquery
                                        GROUP BY user_id, key) AS aggregated
                                  GROUP BY user_id
        )
        INSERT INTO user_stats
           (user_fkey, user_id, general_stats, version, created, updated, details, this_week, last_week, last_30_days, last_90_days)
        SELECT
            'fobj__sb_user_profile__' || aggregated_stats.user_id::varchar,
            aggregated_stats.user_id,
            aggregated_stats.general_stats,
            1,
            NOW(),
            NOW(),
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb

        FROM
            aggregated_stats
        ON CONFLICT (user_id)
        DO UPDATE
        SET
            general_stats = EXCLUDED.general_stats
        '''
    )

    jdbc_connection_options = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_driver_name('org.postgresql.Driver')
        .with_user_name(os.environ['FLINK_DB_USER'])
        .with_password(os.environ['FLINK_DB_PASSWORD'])
        .with_url(
            f'jdbc:postgresql://{os.environ["FLINK_DB_HOST"]}:{os.environ["FLINK_DB_PORT"]}'
            f'/{os.environ["FLINK_DB_NAME"]}',
        )
        .build()
    )
    jdbc_execution_options = (
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(BATCH_INTERVAL_MS)
        .with_batch_size(BATCH_SIZE)
        .with_max_retries(MAX_RETRIES)
        .build()
    )
    jdbc_sink = JdbcSink.sink(
        sql=upsert_query,
        type_info=OUTPUT_TYPE_INFO,
        jdbc_connection_options=jdbc_connection_options,
        jdbc_execution_options=jdbc_execution_options,
    )
    (ds
     .map(StatsRowMapper(), output_type=OUTPUT_TYPE_INFO)
     .uid('picks_mapper_id')
     .add_sink(jdbc_sink)
     .uid('jdbc_sink_id'))


ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    'picks'
)
ds = ds.map(
    PickMapper(),
).process(
    PickProcess(),
)
sink_to_db(ds)

# Execute
env.execute('run-user-stats')
