import logging
from typing import Iterator

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors import FileSink, OutputFileConfig
import json
from pyflink.datastream.functions import MapFunction, ProcessFunction, FlatMapFunction
from dataclasses import dataclass
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors import kafka


@dataclass
class Pick:
    user_fkey: str
    type: int
    currency_code: int
    risk_amount: int
    us_coeff: int
    expected_payout_amount: int
    selections_count: int
    stake_category: int
    settlement_state: int
    settlement_stamp_millis_utc: int
    settlement_payout_amount: int


@dataclass
class UserStats:
    user_fkey: str
    total_risk_amount: int


class PickMapper(MapFunction):
    def map(self, data: str):
        print('map')
        logging.warning('map')
        deserialized_data = json.loads(data)
        fliff_objects = deserialized_data['fliff_objects']
        print(len(fliff_objects))
        logging.warning(len(fliff_objects))
        return [
            Pick(
                user_fkey=fo['user_fkey'],
                type=fo['type'],
                currency_code=fo['currency_code'],
                risk_amount=fo['risk_amount'],
                us_coeff=fo['us_coeff'],
                expected_payout_amount=fo['expected_payout_amount'],
                selections_count=fo['selections_count'],
                stake_category=fo['stake_category'],
                settlement_state=fo['settlement_state'],
                settlement_stamp_millis_utc=fo['settlement_stamp_millis_utc'],
                settlement_payout_amount=fo['settlement_payout_amount'],
            )
            for fo in fliff_objects
        ]


class PickProcess(ProcessFunction):
    def process_element(self, picks: list[Pick], ctx):
        print('process_element')
        logging.warning('process_element')
        user_fkeys = {p.user_fkey for p in picks}
        stats = {}
        for u in user_fkeys:
            u_stats = UserStats(u, 0)
            for p in picks:
                if p.user_fkey == u:
                    u_stats.total_risk_amount += p.risk_amount
            stats[u] = u_stats

        print('before yield')
        logging.warning('before yield')
        yield list(stats.values())
        print('yield ok')
        logging.warning('yield ok')


class PickRowMapper(FlatMapFunction):
    def flat_map(self, data: list[UserStats]) -> Iterator[Row]:
        print('flat_map start')
        logging.warning('flat_map start')
        for st in data:
            yield Row(
                st.user_fkey,
                st.total_risk_amount
            )
            print('flat_map yield ok')
            logging.warning('flat_map yield ok')


# docker run --rm -it confluentinc/cp-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic picks

KAFKA_PROPERTIES = {
    'zookeeper.connect': 'zookeeper:2181',
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-group-ilya',
}
CHECKPOINT_INTERVAL = 1000
CHECKPOINT_TIMEOUT = 6000
MIN_INTERVAL_BETWEEN_CHECKPOINTS = 200

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(CHECKPOINT_INTERVAL)
env.get_checkpoint_config().set_min_pause_between_checkpoints(MIN_INTERVAL_BETWEEN_CHECKPOINTS)
env.get_checkpoint_config().set_checkpoint_timeout(CHECKPOINT_TIMEOUT)


kafka_source = (
    kafka.KafkaSource.builder()
    .set_properties(KAFKA_PROPERTIES)
    .set_bootstrap_servers('broker:29092')
    .set_topics('picks')
    .set_starting_offsets(kafka.KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
print('kafka_source setup ok')

# output_path = '/tmp/picks/main'
# file_sink = FileSink \
#     .for_row_format(output_path, Encoder.simple_string_encoder('UTF-8')) \
#     .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
#     .build()
print('file_sink setup ok')


def sink_to_db(ds: DataStream) -> None:
    BATCH_INTERVAL_MS = 2000
    BATCH_SIZE = 100
    MAX_RETRIES = 5
    OUTPUT_TYPE_INFO = Types.ROW(
        [Types.STRING(), Types.INT()]
    )

    upsert_query = (
        f'''INSERT INTO user_stats
        (user_fkey, risk_amount)
        VALUES (?, ?)
        ON CONFLICT (user_fkey)
        DO UPDATE SET risk_amount = user_stats.risk_amount + EXCLUDED.risk_amount'''
    )

    jdbc_connection_options = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_driver_name('org.postgresql.Driver')
        .with_user_name('postgres')
        .with_password('postgres')
        .with_url(
            f'jdbc:postgresql://postgres:5432/flinkdb')
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
     .flat_map(PickRowMapper(), output_type=OUTPUT_TYPE_INFO)
     .uid('picks_mapper_id')
     .add_sink(jdbc_sink)
     .uid('jdbc_sink_id'))

ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    'picks'
)
ds = ds.map(
    PickMapper()
).process(
    PickProcess()
)
sink_to_db(ds)

# Execute
env.execute("2-picks")
