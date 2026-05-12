import math
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional

import psycopg
import requests
from psycopg.rows import dict_row

from airflow import DAG
from airflow.operators.python import PythonOperator


DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "road_events_db")
DB_USER = os.getenv("DB_USER", "roaduser")
DB_PASSWORD = os.getenv("DB_PASSWORD")

KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY")

RADIUS_METERS = 15.0
BATCH_LIMIT = 5000


def make_cluster_id() -> str:
    now = datetime.now()
    short_uuid = uuid.uuid4().hex[:8]
    return f"cluster_{now.strftime('%Y%m%d%H%M%S')}_{short_uuid}"


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        row_factory=dict_row,
    )


def get_road_address_from_kakao(lat: float, lng: float) -> Optional[str]:
    """
    Kakao Local API를 사용해 좌표를 도로명 주소로 변환합니다.

    x = 경도
    y = 위도

    도로명 주소가 없으면 지번 주소를 fallback으로 사용합니다.
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


def haversine_meters(lat1, lng1, lat2, lng2):
    earth_radius = 6371000

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lng = math.radians(lng2 - lng1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lng / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius * c


def find_nearby_cluster(event, clusters, radius_meters=RADIUS_METERS):
    event_lat = event["gps_lat"]
    event_lng = event["gps_lng"]

    nearest_cluster = None
    nearest_distance = None

    for cluster in clusters:
        distance = haversine_meters(
            event_lat,
            event_lng,
            cluster["gps_lat"],
            cluster["gps_lng"],
        )

        if distance <= radius_meters:
            if nearest_distance is None or distance < nearest_distance:
                nearest_distance = distance
                nearest_cluster = cluster

    return nearest_cluster, nearest_distance


def cluster_pothole_events():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            # 1. 아직 클러스터링되지 않은 P/U_P 후보 이벤트 조회
            # road_address는 events에서 가져오지 않습니다.
            # cluster 생성 시점에 Kakao API로 road_defect_clusters에만 저장합니다.
            cur.execute(
                """
                SELECT
                    event_id,
                    event_timestamp,
                    gps_lat,
                    gps_lng,
                    cp_class,
                    cp_class_name
                FROM road_defect_events
                WHERE is_clustered = 0
                  AND cp_class_name IN ('P', 'U_P')
                ORDER BY event_timestamp ASC
                LIMIT %s;
                """,
                (BATCH_LIMIT,),
            )

            events = cur.fetchall()

            if not events:
                print("No unclustered P/U_P events.")
                return

            print(f"Unclustered events count: {len(events)}")
            print(f"kakao_key_exists={bool(KAKAO_REST_API_KEY)}")

            # 2. 기존 미수리 대표 포트홀 조회
            cur.execute(
                """
                SELECT
                    cluster_id,
                    gps_lat,
                    gps_lng,
                    detection_count,
                    first_event_timestamp,
                    last_event_timestamp
                FROM road_defect_clusters
                WHERE is_repaired = 0;
                """
            )

            clusters = cur.fetchall()

            print(f"Existing unrepaired clusters count: {len(clusters)}")

            new_cluster_count = 0
            merged_event_count = 0

            for event in events:
                nearby_cluster, distance = find_nearby_cluster(event, clusters)

                if nearby_cluster:
                    # 기존 대표 포트홀 15m 이내면 새 cluster를 만들지 않습니다.
                    cluster_id = nearby_cluster["cluster_id"]

                    cur.execute(
                        """
                        UPDATE road_defect_clusters
                        SET
                            detection_count = detection_count + 1,
                            last_event_timestamp = GREATEST(last_event_timestamp, %s),
                            updated_at = NOW()
                        WHERE cluster_id = %s;
                        """,
                        (
                            event["event_timestamp"],
                            cluster_id,
                        ),
                    )

                    cur.execute(
                        """
                        UPDATE road_defect_events
                        SET
                            is_clustered = 1,
                            cluster_id = %s,
                            clustered_at = NOW()
                        WHERE event_id = %s;
                        """,
                        (
                            cluster_id,
                            event["event_id"],
                        ),
                    )

                    nearby_cluster["detection_count"] += 1

                    if event["event_timestamp"] > nearby_cluster["last_event_timestamp"]:
                        nearby_cluster["last_event_timestamp"] = event["event_timestamp"]

                    merged_event_count += 1

                    print(
                        f"Merged event_id={event['event_id']} "
                        f"into cluster_id={cluster_id}, "
                        f"distance={distance:.2f}m"
                    )

                else:
                    # 기존 대표 포트홀 15m 이내에 없으면 새 대표 포트홀 생성
                    new_cluster_id = make_cluster_id()

                    # 여기서만 Kakao API 호출
                    road_address = get_road_address_from_kakao(
                        lat=float(event["gps_lat"]),
                        lng=float(event["gps_lng"]),
                    )

                    cur.execute(
                        """
                        INSERT INTO road_defect_clusters (
                            cluster_id,
                            representative_event_id,
                            first_event_timestamp,
                            last_event_timestamp,
                            gps_lat,
                            gps_lng,
                            cp_class,
                            cp_class_name,
                            road_address,
                            detection_count,
                            is_repaired
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 0);
                        """,
                        (
                            new_cluster_id,
                            event["event_id"],
                            event["event_timestamp"],
                            event["event_timestamp"],
                            event["gps_lat"],
                            event["gps_lng"],
                            event["cp_class"],
                            event["cp_class_name"],
                            road_address,
                        ),
                    )

                    cur.execute(
                        """
                        UPDATE road_defect_events
                        SET
                            is_clustered = 1,
                            cluster_id = %s,
                            clustered_at = NOW()
                        WHERE event_id = %s;
                        """,
                        (
                            new_cluster_id,
                            event["event_id"],
                        ),
                    )

                    clusters.append(
                        {
                            "cluster_id": new_cluster_id,
                            "gps_lat": event["gps_lat"],
                            "gps_lng": event["gps_lng"],
                            "detection_count": 1,
                            "first_event_timestamp": event["event_timestamp"],
                            "last_event_timestamp": event["event_timestamp"],
                        }
                    )

                    new_cluster_count += 1

                    print(
                        f"Created new cluster_id={new_cluster_id} "
                        f"from event_id={event['event_id']}, "
                        f"road_address={road_address}"
                    )

            conn.commit()

            print("Cluster processing completed.")
            print(f"New clusters inserted: {new_cluster_count}")
            print(f"Events merged into existing clusters: {merged_event_count}")

    except Exception as e:
        conn.rollback()
        print(f"Cluster processing failed: {e}")
        raise

    finally:
        conn.close()


default_args = {
    "owner": "pothole-pjt",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="cluster_potholes_every_12min",
    default_args=default_args,
    description="Cluster P/U_P road defect events into representative potholes every 12 minutes",
    start_date=datetime(2026, 5, 1),
    schedule_interval="*/12 * * * *",
    catchup=False,
    tags=["pothole", "clustering", "rds", "kakao"],
) as dag:

    cluster_task = PythonOperator(
        task_id="cluster_pothole_events",
        python_callable=cluster_pothole_events,
    )
