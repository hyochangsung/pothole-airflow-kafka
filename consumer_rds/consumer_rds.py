import json
import os
import time
from typing import Any, Dict, Optional

import psycopg2
from confluent_kafka import Consumer, KafkaException
import requests


KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.logs")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-log-rds-consumer-group")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "road_events_db")
DB_USER = os.getenv("DB_USER", "roaduser")
DB_PASSWORD = os.getenv("DB_PASSWORD")

TARGET_CLASSES = {"P", "U_P"}


def get_road_address_from_kakao(lat: float, lng: float) -> Optional[str]:
    """
    Kakao Local API를 사용해 WGS84 좌표를 도로명 주소로 변환합니다.

    Kakao API:
    GET https://dapi.kakao.com/v2/local/geo/coord2address.json

    x = 경도
    y = 위도
    """

    if not KAKAO_REST_API_KEY:
        print("KAKAO_REST_API_KEY is not set")
        return None

    url = "https://dapi.kakao.com/v2/local/geo/coord2address.json"

    headers = {
        "Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"
    }

    params = {
        "x": lng,
        "y": lat,
        "input_coord": "WGS84",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=3,
        )

        if response.status_code != 200:
            print(
                "Kakao API request failed:",
                response.status_code,
                response.text[:500],
            )
            return None

        data = response.json()
        documents = data.get("documents", [])

        if not documents:
            return None

        doc = documents[0]

        road_address = doc.get("road_address")
        lot_address = doc.get("address")

        if road_address and road_address.get("address_name"):
            return road_address["address_name"]

        if lot_address and lot_address.get("address_name"):
            return lot_address["address_name"]

        return None

    except Exception as e:
        print(f"Kakao address API error: {e}")
        return None


def create_kafka_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def create_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def is_target_event(event: Dict[str, Any]) -> bool:
    return event.get("cp_class_name") in TARGET_CLASSES


def make_event_id(event: Dict[str, Any], msg) -> str:
    if event.get("event_id"):
        return str(event["event_id"])

    return f"{msg.topic()}-{msg.partition()}-{msg.offset()}"


def validate_event(event: Dict[str, Any]) -> bool:
    required_fields = [
        "timestamp",
        "gps_lat",
        "gps_lng",
        "cp_class",
        "cp_class_name",
    ]

    for field in required_fields:
        if event.get(field) is None:
            return False

    return True


def insert_event(conn, event: Dict[str, Any], event_id: str):
    road_address = get_road_address_from_kakao(
        lat=float(event["gps_lat"]),
        lng=float(event["gps_lng"]),
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO road_defect_events (
                event_id,
                event_timestamp,
                gps_lat,
                gps_lng,
                cp_class,
                cp_class_name,
                road_address
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (
                event_id,
                event["timestamp"],
                event["gps_lat"],
                event["gps_lng"],
                event["cp_class"],
                event["cp_class_name"],
                road_address,
            ),
        )

    conn.commit()

    return road_address


def main():
    consumer = create_kafka_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    db_conn = create_db_connection()

    print("RDS Consumer started")
    print(f"topic={KAFKA_TOPIC}")
    print(f"db_host={DB_HOST}")
    print(f"db_name={DB_NAME}")
    print(f"kakao_key_exists={bool(KAKAO_REST_API_KEY)}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                print("Invalid JSON skipped")
                consumer.commit(msg)
                continue

            event_id = make_event_id(event, msg)

            if not is_target_event(event):
                print(
                    f"Skipped: event_id={event_id}, "
                    f"cp_class_name={event.get('cp_class_name')}"
                )
                consumer.commit(msg)
                continue

            if not validate_event(event):
                print(
                    f"Invalid target event skipped: "
                    f"event_id={event_id}, event={event}"
                )
                consumer.commit(msg)
                continue

            try:
                road_address = insert_event(db_conn, event, event_id)

                consumer.commit(msg)

                print(
                    f"Inserted: event_id={event_id}, "
                    f"timestamp={event['timestamp']}, "
                    f"lat={event['gps_lat']}, "
                    f"lng={event['gps_lng']}, "
                    f"cp_class={event['cp_class']}, "
                    f"cp_class_name={event['cp_class_name']}, "
                    f"road_address={road_address}"
                )

            except psycopg2.OperationalError as e:
                print("DB connection error:", e)

                try:
                    db_conn.close()
                except Exception:
                    pass

                time.sleep(5)
                db_conn = create_db_connection()

            except Exception as e:
                print(f"Insert error: event_id={event_id}, error={e}")
                db_conn.rollback()
                time.sleep(1)

    finally:
        consumer.close()
        db_conn.close()


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print("RDS Consumer crashed:", e)
            print("Restart after 5 seconds")
            time.sleep(5)
