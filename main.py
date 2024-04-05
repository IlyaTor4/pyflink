from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors import FileSink, OutputFileConfig
import json
from pyflink.datastream.functions import MapFunction, ProcessFunction
from dataclasses import dataclass
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
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
        deserialized_data = json.loads(data)
        fliff_objects = deserialized_data['fliff_objects']
        print(len(fliff_objects))
        return [
            Pick(**fo)
            for fo in fliff_objects
        ]


class PickProcess(ProcessFunction):
    def process_element(self, picks: list[Pick], ctx):
        print('process_element')
        user_fkeys = {p.user_fkey for p in picks}
        stats = {}
        for u in user_fkeys:
            u_stats = UserStats(u, 0)
            for p in picks:
                if p.user_fkey == u:
                    u_stats.total_risk_amount += p.risk_amount
            stats[u] = u_stats

        yield list(stats.values())
        print('yield ok')


# docker run --rm -it confluentinc/cp-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic picks

KAFKA_PROPERTIES = {
    'bootstrap.servers': 'kafka:9092',
    'zookeeper.connect': 'zookeeper:2181',
    'group.id': 'flink-group-ilya',
}
CHECKPOINT_INTERVAL = 1000
CHECKPOINT_TIMEOUT = 6000
MIN_INTERVAL_BETWEEN_CHECKPOINTS = 200

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(CHECKPOINT_INTERVAL)
env.get_checkpoint_config().set_min_pause_between_checkpoints(MIN_INTERVAL_BETWEEN_CHECKPOINTS)
env.get_checkpoint_config().set_checkpoint_timeout(CHECKPOINT_TIMEOUT)
print('env setup ok')

kafka_source = (
    kafka.KafkaSource.builder()
    .set_properties(KAFKA_PROPERTIES)
    .set_topics('picks')
    .set_starting_offsets(kafka.KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
print('kafka_source setup ok')

output_path = '/tmp/picks/main'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()
print('file_sink setup ok')


ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    'picks'
)
ds.map(
    PickMapper()
).process(
    PickProcess()
).sink_to(
    file_sink
)
print('ds setup ok')

# Execute
env.execute("2-picks")
print('execute done')
