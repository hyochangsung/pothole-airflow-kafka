import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Dict, List

import boto3
import pandas as pd
from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.logs")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-log-s3-consumer-group")

S3_BUCKET = os.getenv("S3_BUCKET", "road-defect-seong")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/raw_logs")

# 12분마다 S3에 저장
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "720"))

# 서울 시간대 UTC+9
KST = timezone(timedelta(hours=9))

s3 = boto3.client("s3")


def create_kafka_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def normalize_event(event: Dict[str, Any], msg) -> Dict[str, Any]:
    """
    Kafka JSON 로그를 Parquet에 저장하기 좋은 평평한 구조로 변환합니다.
    없는 값은 None으로 들어갑니다.
    """
    return {
        "event_id": event.get("event_id"),
        "frame_id": event.get("frame_id"),
        "timestamp": event.get("timestamp"),
        "source": event.get("source"),
        "run_id": event.get("run_id"),
        "bus_id": event.get("bus_id"),
        "direction": event.get("direction"),

        "prob_normal": event.get("prob_normal"),
        "prob_pothole": event.get("prob_pothole"),
        "confidence": event.get("confidence"),
        "cp_class": event.get("cp_class"),
        "cp_class_name": event.get("cp_class_name"),
        "inference_time_ms": event.get("inference_time_ms"),

        "gps_lat": event.get("gps_lat"),
        "gps_lng": event.get("gps_lng"),
        "speed_kmh": event.get("speed_kmh"),

        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),

        # 적재 시각도 서울 시간 기준으로 저장
        "ingested_at": datetime.now(KST).isoformat(),

        "parse_error": False,
    }


def normalize_invalid_json(raw_value: str, msg) -> Dict[str, Any]:
    """
    JSON 파싱 실패 메시지도 버리지 않고 S3에 남깁니다.
    """
    return {
        "event_id": None,
        "frame_id": None,
        "timestamp": None,
        "source": None,
        "run_id": None,
        "bus_id": None,
        "direction": None,

        "prob_normal": None,
        "prob_pothole": None,
        "confidence": None,
        "cp_class": None,
        "cp_class_name": None,
        "inference_time_ms": None,

        "gps_lat": None,
        "gps_lng": None,
        "speed_kmh": None,

        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),

        # 적재 시각도 서울 시간 기준
        "ingested_at": datetime.now(KST).isoformat(),

        "parse_error": True,
        "raw_value": raw_value,
    }


def make_s3_key() -> str:
    """
    S3 저장 경로를 서울 시간 기준으로 생성합니다.
    예:
    raw/raw_logs/year=2026/month=05/day=04/hour=22/part-xxxx.parquet
    """
    now = datetime.now(KST)

    return (
        f"{S3_PREFIX}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"part-{uuid.uuid4().hex}.parquet"
    )


def upload_batch_to_s3_as_parquet(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    df = pd.DataFrame(rows)

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False,
        compression="snappy",
    )

    buffer.seek(0)

    key = make_s3_key()

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Uploaded parquet: s3://{S3_BUCKET}/{key}, rows={len(rows)}")


def main():
    consumer = create_kafka_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    buffer: List[Dict[str, Any]] = []
    last_flush_time = time.time()

    print("S3 Parquet Consumer started")
    print(f"topic={KAFKA_TOPIC}")
    print(f"bucket={S3_BUCKET}")
    print(f"prefix={S3_PREFIX}")
    print(f"flush_interval_seconds={FLUSH_INTERVAL_SECONDS}")
    print("timezone=Asia/Seoul UTC+9")

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())

                raw_value = msg.value().decode("utf-8")

                try:
                    event = json.loads(raw_value)
                    row = normalize_event(event, msg)
                except json.JSONDecodeError:
                    row = normalize_invalid_json(raw_value, msg)

                buffer.append(row)

            # 1시간마다 저장
            should_flush_by_time = now - last_flush_time >= FLUSH_INTERVAL_SECONDS

            if buffer and should_flush_by_time:
                upload_batch_to_s3_as_parquet(buffer)

                # S3 저장 성공 후 offset commit
                consumer.commit()

                buffer.clear()
                last_flush_time = now

    finally:
        if buffer:
            upload_batch_to_s3_as_parquet(buffer)
            consumer.commit()

        consumer.close()


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print("S3 Parquet Consumer crashed:", e)
            print("Restart after 5 seconds")
            time.sleep(5)
