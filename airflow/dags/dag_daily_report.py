import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import boto3
import pandas as pd
import io
import logging
import psycopg
from psycopg.rows import dict_row



# ── 설정 ──────────────────────────────────────────────────────
BUCKET = "road-defect-seong"
RAW_PREFIX = "raw/raw_logs"
REPORT_PREFIX = "reports"
REGION = "ap-northeast-1"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "road_events_db")
DB_USER = os.getenv("DB_USER", "roaduser")
DB_PASSWORD = os.getenv("DB_PASSWORD")

RUN_ORDER = ["first", "middle", "last"]

KST = ZoneInfo("Asia/Seoul")

# ── 헬퍼 함수 ─────────────────────────────────────────────────

def get_s3_client():
    return boto3.client("s3", region_name=REGION)


def get_db_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        row_factory=dict_row,
    )


def get_day_range(date_str: str):
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    start_at = datetime.combine(d, time.min, tzinfo=KST)
    end_at = start_at + timedelta(days=1)
    return start_at, end_at


def load_and_clean_parquet(target_date: str) -> pd.DataFrame:
    """S3에서 날짜별 Parquet 로드 후 전처리"""
    s3 = get_s3_client()
    year, month, day = target_date.split("-")
    prefix = f"{RAW_PREFIX}/year={year}/month={month}/day={day}/"

    logging.info(f"S3 조회: s3://{BUCKET}/{prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    dfs = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            response = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            dfs.append(df)

    if not dfs:
        raise ValueError(f"{target_date} 날짜의 Parquet 파일이 없습니다.")

    df = pd.concat(dfs, ignore_index=True)
    raw_count = len(df)

    df = df[df["parse_error"] == False].copy()
    error_count = raw_count - len(df)

    df["cp_class"] = df["cp_class"].fillna(0).astype(int)
    df["frame_id"] = df["frame_id"].fillna(0).astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["hour"] = df["timestamp"].dt.hour

    logging.info(f"로드 완료: {len(df):,}건 (parse_error 제거: {error_count}건)")
    return df


def upload_csv_to_s3(df: pd.DataFrame, s3_key: str):
    """DataFrame을 CSV로 변환해 S3에 저장"""
    s3 = get_s3_client()
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    s3.put_object(Bucket=BUCKET, Key=s3_key,
                  Body=buf.getvalue().encode("utf-8-sig"),
                  ContentType="text/csv")
    logging.info(f"CSV 업로드: s3://{BUCKET}/{s3_key}")


# ── Task 1: 모델 성능 모니터링 ────────────────────────────────

def task_model_performance(**context):
    """회차별 평균 추론 시간 + CP 클래스 분포 집계"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    logging.info(f"[Task 1] 시작: {target_date}")

    df = load_and_clean_parquet(target_date)

    run_stats = df.groupby("run_id")["inference_time_ms"].mean().round(2)
    cp_counts = df["cp_class"].value_counts().sort_index()

    logging.info(f"[Task 1] 완료. {len(df):,}건")
    return {"total": len(df), "date": target_date}


# ── Task 2: 속도별 불확실 예측 비율 분석 ─────────────────────

def task_speed_uncertainty(**context):
    """속도 구간별 불확실 예측(U_N + U_P) 비율 집계"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    logging.info(f"[Task 2] 시작: {target_date}")

    df = load_and_clean_parquet(target_date)

    df["is_uncertain"] = df["cp_class"].isin([1, 2]).astype(int)

    bins = [0, 10, 20, 30, 36]
    bin_labels = ["0~10 km/h", "10~20 km/h", "20~30 km/h", "30+ km/h"]
    df["speed_bin"] = pd.cut(df["speed_kmh"], bins=bins, labels=bin_labels, right=False)

    speed_stats = (
        df.groupby("speed_bin", observed=True)
        .agg(total=("is_uncertain", "count"), uncertain=("is_uncertain", "sum"))
        .reset_index()
    )
    speed_stats["uncertain_ratio"] = (
        speed_stats["uncertain"] / speed_stats["total"] * 100
    ).round(2)

    logging.info(f"[Task 2] 완료")
    return {"date": target_date}


# ── Task 3: HNM 후보 추출 ────────────────────────────────────

def task_hnm_candidates(**context):
    """U_N, U_P 전체를 HNM 후보로 추출해 CSV로 S3 저장"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    logging.info(f"[Task 3] 시작: {target_date}")

    df = load_and_clean_parquet(target_date)

    cols = ["event_id", "timestamp", "bus_id", "run_id",
            "cp_class_name", "confidence", "gps_lat", "gps_lng",
            "speed_kmh", "inference_time_ms"]

    un_candidates = df[df["cp_class"] == 1][cols].copy()
    up_candidates = df[df["cp_class"] == 2][cols].copy()

    un_candidates["hnm_type"] = "U_N"
    up_candidates["hnm_type"] = "U_P"

    all_candidates = pd.concat([un_candidates, up_candidates], ignore_index=True)

    upload_csv_to_s3(all_candidates,
                     f"{REPORT_PREFIX}/{target_date}/task3_hnm_candidates.csv")

    logging.info(f"[Task 3] 완료. 총 {len(all_candidates)}건")
    return {"date": target_date, "un": len(un_candidates),
            "up": len(up_candidates), "total": len(all_candidates)}


# ── Task 4: 포트홀 클러스터 일일 집계 ────────────────────────

def task_pothole_cluster_report(**context):
    """RDS에서 당일 포트홀 클러스터 집계 및 전일 대비 증감률 계산"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    yesterday_date = (
        datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    logging.info(f"[Task 4] 시작: {target_date}")

    today_start, today_end = get_day_range(target_date)
    yesterday_start, yesterday_end = get_day_range(yesterday_date)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT COUNT(*) AS count FROM road_defect_clusters
                WHERE created_at >= %s AND created_at < %s;
                """,
                (today_start, today_end),
            )
            today_new_count = cur.fetchone()["count"]

            cur.execute(
                """
                SELECT COUNT(*) AS count FROM road_defect_clusters
                WHERE created_at >= %s AND created_at < %s;
                """,
                (yesterday_start, yesterday_end),
            )
            yesterday_new_count = cur.fetchone()["count"]

            cur.execute("SELECT COUNT(*) AS count FROM road_defect_clusters;")
            total_count = cur.fetchone()["count"]

            cur.execute(
                "SELECT COUNT(*) AS count FROM road_defect_clusters WHERE is_repaired = 0;"
            )
            unrepaired_count = cur.fetchone()["count"]

            cur.execute(
                "SELECT COUNT(*) AS count FROM road_defect_clusters WHERE is_repaired = 1;"
            )
            repaired_count = cur.fetchone()["count"]

            cur.execute(
                """
                SELECT cluster_id, representative_event_id,
                       first_event_timestamp, last_event_timestamp,
                       gps_lat, gps_lng, cp_class, cp_class_name,
                       road_address, detection_count, is_repaired
                FROM road_defect_clusters
                WHERE created_at >= %s AND created_at < %s
                ORDER BY created_at ASC;
                """,
                (today_start, today_end),
            )
            today_new_clusters = cur.fetchall()

    finally:
        conn.close()

    diff = today_new_count - yesterday_new_count
    change_rate = (
        round(diff / yesterday_new_count * 100, 2)
        if yesterday_new_count > 0 else None
    )

    result = {
        "today_new_cluster_count": today_new_count,
        "yesterday_new_cluster_count": yesterday_new_count,
        "diff_from_yesterday": diff,
        "change_rate_percent": change_rate,
        "total_cluster_count": total_count,
        "total_unrepaired_cluster_count": unrepaired_count,
        "total_repaired_cluster_count": repaired_count,
        "today_new_clusters": [
            {
                "cluster_id": r["cluster_id"],
                "representative_event_id": r["representative_event_id"],
                "first_event_timestamp": r["first_event_timestamp"].isoformat()
                    if r["first_event_timestamp"] else None,
                "last_event_timestamp": r["last_event_timestamp"].isoformat()
                    if r["last_event_timestamp"] else None,
                "gps_lat": r["gps_lat"],
                "gps_lng": r["gps_lng"],
                "cp_class": r["cp_class"],
                "cp_class_name": r["cp_class_name"],
                "road_address": r.get("road_address"),
                "detection_count": r["detection_count"],
                "is_repaired": r["is_repaired"],
            }
            for r in today_new_clusters
        ],
    }

    logging.info(f"[Task 4] 완료. 오늘 신규: {today_new_count}건, 전체: {total_count}건")
    return result


# ── Task 5: 일일 요약 JSON 생성 ──────────────────────────────

def task_generate_summary(**context):
    """Task 1~4 결과를 취합해 summary.json으로 S3 저장"""
    target_date = datetime.now(KST).strftime("%Y-%m-%d")
    logging.info(f"[Task 5] 시작: {target_date}")

    ti = context["ti"]
    cluster_data = ti.xcom_pull(task_ids="pothole_cluster_report")

    df = load_and_clean_parquet(target_date)

    # CP 클래스 분포
    cp_counts = df["cp_class"].value_counts().sort_index()
    cp_distribution = {str(i): int(cp_counts.get(i, 0)) for i in range(4)}

    # 운행 회차별 평균 추론 시간
    run_stats = df.groupby("run_id")["inference_time_ms"].mean().round(2)
    run_inference_time = {
        run: float(run_stats.get(run, 0.0))
        for run in RUN_ORDER
    }

    # 속도 구간별 불확실 예측 비율
    df["is_uncertain"] = df["cp_class"].isin([1, 2]).astype(int)
    bins = [0, 10, 20, 30, 36]
    bin_labels = ["0~10 km/h", "10~20 km/h", "20~30 km/h", "30+ km/h"]
    df["speed_bin"] = pd.cut(df["speed_kmh"], bins=bins, labels=bin_labels, right=False)
    speed_stats = (
        df.groupby("speed_bin", observed=True)
        .agg(total=("is_uncertain", "count"), uncertain=("is_uncertain", "sum"))
    )
    speed_uncertainty = {}
    for label in bin_labels:
        if label in speed_stats.index and speed_stats.loc[label, "total"] > 0:
            speed_uncertainty[label] = {
                "total": int(speed_stats.loc[label, "total"]),
                "uncertain_ratio": round(
                    float(speed_stats.loc[label, "uncertain"] /
                          speed_stats.loc[label, "total"] * 100), 2
                )
            }
        else:
            speed_uncertainty[label] = {"total": 0, "uncertain_ratio": 0.0}

    # HNM 후보 수
    un_count = int((df["cp_class"] == 1).sum())
    up_count = int((df["cp_class"] == 2).sum())

    summary = {
        "date": target_date,
        "cp_distribution": cp_distribution,
        "run_inference_time": run_inference_time,
        "speed_uncertainty": speed_uncertainty,
        "hnm_candidates": {
            "un": un_count,
            "up": up_count,
            "total": un_count + up_count,
        },
        "pothole_clusters": cluster_data,
        "generated_at": datetime.now(KST).isoformat(),
    }

    s3 = get_s3_client()
    s3_key = f"{REPORT_PREFIX}/{target_date}/summary.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logging.info(f"[Task 5] 완료: s3://{BUCKET}/{s3_key}")
    return summary


# ── DAG 정의 ──────────────────────────────────────────────────

default_args = {
    "owner": "pothole-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_daily_report",
    default_args=default_args,
    description="매일 KST 23:50 S3 Parquet 분석 + RDS 포트홀 집계 → 일일 리포트 생성",
    schedule_interval="50 23 * * *",
    start_date=datetime(2026, 5, 1, tzinfo=KST),
    catchup=False,
    tags=["daily", "report", "analysis", "cluster"],
) as dag:

    t1 = PythonOperator(
        task_id="model_performance_monitoring",
        python_callable=task_model_performance,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="speed_uncertainty_analysis",
        python_callable=task_speed_uncertainty,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="hnm_candidates_extraction",
        python_callable=task_hnm_candidates,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="pothole_cluster_report",
        python_callable=task_pothole_cluster_report,
        provide_context=True,
    )

    t5 = PythonOperator(
        task_id="generate_daily_summary",
        python_callable=task_generate_summary,
        provide_context=True,
    )

    t1 >> t2 >> t3 >> t4 >> t5
