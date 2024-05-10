from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime
from pyflink.table import Kafka, Json, Rowtime
from pyflink.table.window import Tumble
from datetime import timedelta

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    tenv = StreamTableEnvironment.create(env, settings)

    # Define the source table with a descriptor
    tenv.connect(
        Kafka()
        .version("universal")
        .topic("picks")
        .start_from_earliest()
        .property("bootstrap.servers", "broker:29092")
        .property("group.id", "flink_picks")
    ).with_format(
        Json()
    ).with_schema(
        Schema()
        .field("user_fkey", DataTypes.STRING())
        .field("type", DataTypes.INT())
        .field("currency_code", DataTypes.INT())
        .field("risk_amount", DataTypes.INT())
        .field("us_coeff", DataTypes.INT())
        .field("expected_payout_amount", DataTypes.INT())
        .field("selections_count", DataTypes.INT())
        .field("stake_category", DataTypes.INT())
        .field("settlement_state", DataTypes.INT())
        .field("settlement_stamp_millis_utc", DataTypes.INT())
        .field("settlement_payout_amount", DataTypes.INT())
        .field("proctime", DataTypes.TIMESTAMP(3)).proctime()
    ).create_temporary_table("picks_data")

    # Create a Table object from the source table
    picks_table = tenv.from_path("picks_data")

    # Define the window operation
    windowed_table = picks_table.window(
        Tumble.over(timedelta(seconds=30)).on("proctime").alias("w")
    ).group_by(
        "w, user_fkey"
    ).select(
        "user_fkey, w.start as window_start, w.end as window_end, risk_amount.sum as total_risk_amount"
    )

    # Define the sink table with a descriptor
    tenv.connect(
        Kafka()
        .version("universal")
        .topic("user_stats_window")
        .property("bootstrap.servers", "broker:29092")
        .property("group.id", "flink_picks")
    ).with_format(
        Json()
    ).with_schema(
        Schema()
        .field("user_fkey", DataTypes.STRING())
        .field("window_start", DataTypes.TIMESTAMP(3))
        .field("window_end", DataTypes.TIMESTAMP(3))
        .field("total_risk_amount", DataTypes.FLOAT())
    ).create_temporary_table("user_stats_window")

    # Execute the insertion from the windowed table to the sink table
    windowed_table.execute_insert("user_stats_window").wait()

if __name__ == '__main__':
    main()
