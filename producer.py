import random
from time import sleep
from json import dumps
from kafka import KafkaProducer


bootstrap_servers = ['0.0.0.0:9092']  # python local, broker in container
# bootstrap_servers = ['broker:29092']  # python in container, broker in container
topic = 'picks'


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: dumps(x).encode()
)

for e in range(100):
    pick_id = random.randint(90, 100)
    print(f'producing {pick_id} to topic {topic}')
    data = {
                "__class_name": "Fliff_Social_Object__PickV3__Updated",
                "copied_from_pick_fkey": "",
                "created_stamp_millis_utc": 1712235175054,
                "currency_code": 331,
                "debug_info_caller": "N/A",
                "expected_payout_amount": 261,
                "fkey": f"fobj__sb_user_pick__{pick_id}",
                "risk_amount": 100,
                "selections": [
                    {
                        "channel_id": 441,
                        "channel_name": "MLB",
                        "created_stamp_millis_utc": 1712235175056,
                        "event__away_team_name": "Cleveland Guardians",
                        "event__home_team_name": "Minnesota Twins",
                        "event__is_home_visually_first":False,
                        "event_fkey": "215252_c_p_203_prematch",
                        "event_info": "Cleveland Guardians vs Minnesota Twins",
                        "event_start_stamp_millis_utc": 1712261400000,
                        "fkey": "fobj__sb_user_pick_selection__704990652",
                        "market_fkey": "94e4c71c",
                        "market_name": "Moneyline",
                        "market_note": "",
                        "pick_fkey": f"fobj__sb_user_pick__{pick_id}",
                        "proposal__is_inplay":False,
                        "proposal__market_category": 73201,
                        "proposal__market_type": 9001,
                        "proposal__selection_type": 7701,
                        "proposal_fkey": "757761_p_203_prematch",
                        "selection_name": "Minnesota Twins",
                        "selection_param_1": "",
                        "settlement_stamp_millis_utc": 0,
                        "settlement_state": 501,
                        "sport_id": 3901,
                        "updated_stamp_millis_utc": 1712235175056,
                        "us_coeff": -155,
                        "version": 1
                    },
                    {
                        "channel_id": 441,
                        "channel_name": "MLB",
                        "created_stamp_millis_utc": 1712235175058,
                        "event__away_team_name": "Chicago White Sox",
                        "event__home_team_name": "Kansas City Royals",
                        "event__is_home_visually_first":False,
                        "event_fkey": "215250_c_p_203_prematch",
                        "event_info": "Chicago White Sox vs Kansas City Royals",
                        "event_start_stamp_millis_utc": 1712274000000,
                        "fkey": "fobj__sb_user_pick_selection__704990653",
                        "market_fkey": "6e5e172a",
                        "market_name": "Moneyline",
                        "market_note": "",
                        "pick_fkey": f"fobj__sb_user_pick__{pick_id}",
                        "proposal__is_inplay":False,
                        "proposal__market_category": 73201,
                        "proposal__market_type": 9001,
                        "proposal__selection_type": 7701,
                        "proposal_fkey": "757767_p_203_prematch",
                        "selection_name": "Kansas City Royals",
                        "selection_param_1": "",
                        "settlement_stamp_millis_utc": 0,
                        "settlement_state": 501,
                        "sport_id": 3901,
                        "updated_stamp_millis_utc": 1712235175058,
                        "us_coeff": -170,
                        "version": 1
                    }
                ],
                "selections_count": 2,
                "settlement_payout_amount": 0,
                "settlement_stamp_millis_utc": 0,
                "settlement_state": 501,
                "stake_category": 2901,
                "type": 82,
                "updated_stamp_millis_utc": 1712235175059,
                "us_coeff": 161,
                "user_fkey": f"fobj__sb_user_profile__{pick_id}",
                "version": 2
            }
    producer.send(topic, value=data)
    sleep(1)
