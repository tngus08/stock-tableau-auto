"""
미국 주식 일별 데이터를 Alpha Vantage API로 수집하고,
Tableau에서 사용할 수 있는 .hyper 파일로 저장한 뒤,
선택적으로 Tableau Cloud에 업로드하는 코드입니다.

처음 실행할 때는 .env의 PUBLISH_TO_TABLEAU=false 상태로 두고,
stock_daily.hyper 파일이 정상 생성되는지 먼저 확인하세요.
"""

import os
import time
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    TableDefinition,
    SqlType,
    Inserter,
    TableName,
)

import tableauserverclient as TSC


# =========================
# 1. 기본 설정
# =========================

# 수집할 미국 주식 종목
TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA", "SPY"]

# 생성할 Hyper 파일 이름
HYPER_FILE_NAME = "stock_daily.hyper"

# Alpha Vantage 무료 API 호출 제한을 고려해 종목 간 대기 시간 설정
# 무료 플랜에서는 너무 빠르게 여러 번 호출하면 제한에 걸릴 수 있음
API_SLEEP_SECONDS = 15


# =========================
# 2. 환경변수 불러오기
# =========================

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

TABLEAU_SERVER_URL = os.getenv("TABLEAU_SERVER_URL")
TABLEAU_SITE_ID = os.getenv("TABLEAU_SITE_ID")
TABLEAU_TOKEN_NAME = os.getenv("TABLEAU_TOKEN_NAME")
TABLEAU_TOKEN_VALUE = os.getenv("TABLEAU_TOKEN_VALUE")
TABLEAU_PROJECT_NAME = os.getenv("TABLEAU_PROJECT_NAME", "Default")

# false면 Tableau 업로드 없이 Hyper 파일만 생성
PUBLISH_TO_TABLEAU = os.getenv("PUBLISH_TO_TABLEAU", "false").lower() == "true"


# =========================
# 3. Alpha Vantage 데이터 수집 함수
# =========================

def fetch_daily_stock_data(ticker: str) -> pd.DataFrame:
    """
    Alpha Vantage에서 특정 종목의 일별 주가 데이터를 가져옵니다.

    Parameters
    ----------
    ticker : str
        예: AAPL, MSFT, NVDA

    Returns
    -------
    pd.DataFrame
        date, ticker, open, high, low, close, volume, collected_at 컬럼을 가진 데이터프레임
    """

    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY가 .env 파일에 설정되어 있지 않습니다.")

    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",  # 최근 약 100개 거래일
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    print(f"[수집 시작] {ticker}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # API 제한 또는 오류 메시지 확인
    if "Error Message" in data:
        raise ValueError(f"{ticker} API 오류: {data['Error Message']}")

    if "Note" in data:
        raise RuntimeError(f"{ticker} API 호출 제한 가능성: {data['Note']}")

    time_series = data.get("Time Series (Daily)")

    if not time_series:
        raise ValueError(f"{ticker} 데이터가 없습니다. 응답 내용: {data}")

    rows = []

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for date_str, values in time_series.items():
        rows.append({
            "date": date_str,
            "ticker": ticker,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "collected_at": collected_at,
        })

    df = pd.DataFrame(rows)

    # 날짜형으로 변환 후 오래된 날짜 → 최신 날짜 순서로 정렬
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    print(f"[수집 완료] {ticker}: {len(df)}행")

    return df


def collect_all_stock_data() -> pd.DataFrame:
    """
    TICKERS 목록의 모든 종목 데이터를 수집하여 하나의 DataFrame으로 합칩니다.
    """

    all_data = []

    for idx, ticker in enumerate(TICKERS):
        df = fetch_daily_stock_data(ticker)
        all_data.append(df)

        # 마지막 종목 이후에는 대기하지 않음
        if idx < len(TICKERS) - 1:
            print(f"{API_SLEEP_SECONDS}초 대기 중...")
            time.sleep(API_SLEEP_SECONDS)

    result = pd.concat(all_data, ignore_index=True)

    print(f"[전체 수집 완료] 총 {len(result)}행")

    return result


# =========================
# 4. Hyper 파일 생성 함수
# =========================

def create_hyper_file(df: pd.DataFrame, hyper_file_path: str) -> None:
    """
    pandas DataFrame을 Tableau용 .hyper 파일로 저장합니다.

    Parameters
    ----------
    df : pd.DataFrame
        주가 데이터
    hyper_file_path : str
        저장할 hyper 파일 경로
    """

    hyper_path = Path(hyper_file_path)

    # 기존 hyper 파일이 있으면 삭제 후 새로 생성
    if hyper_path.exists():
        hyper_path.unlink()

    table_name = TableName("Extract", "StockDaily")

    table_definition = TableDefinition(
        table_name=table_name,
        columns=[
            TableDefinition.Column("date", SqlType.date()),
            TableDefinition.Column("ticker", SqlType.text()),
            TableDefinition.Column("open", SqlType.double()),
            TableDefinition.Column("high", SqlType.double()),
            TableDefinition.Column("low", SqlType.double()),
            TableDefinition.Column("close", SqlType.double()),
            TableDefinition.Column("volume", SqlType.big_int()),
            TableDefinition.Column("collected_at", SqlType.text()),
        ],
    )

    print("[Hyper 생성 시작]")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_file_path,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:

            # Extract 스키마 생성
            connection.catalog.create_schema("Extract")

            # StockDaily 테이블 생성
            connection.catalog.create_table(table_definition)

            # DataFrame 행을 Hyper에 삽입
            rows = [
                [
                    row["date"],
                    row["ticker"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["collected_at"],
                ]
                for _, row in df.iterrows()
            ]

            with Inserter(connection, table_definition) as inserter:
                inserter.add_rows(rows)
                inserter.execute()

    print(f"[Hyper 생성 완료] {hyper_file_path}")


# =========================
# 5. Tableau Cloud 업로드 함수
# =========================

def publish_hyper_to_tableau(hyper_file_path: str) -> None:
    """
    생성된 .hyper 파일을 Tableau Cloud 데이터 원본으로 게시합니다.
    동일한 이름의 데이터 원본이 있으면 덮어쓰기합니다.

    Parameters
    ----------
    hyper_file_path : str
        업로드할 hyper 파일 경로
    """

    required_values = {
        "TABLEAU_SERVER_URL": TABLEAU_SERVER_URL,
        "TABLEAU_SITE_ID": TABLEAU_SITE_ID,
        "TABLEAU_TOKEN_NAME": TABLEAU_TOKEN_NAME,
        "TABLEAU_TOKEN_VALUE": TABLEAU_TOKEN_VALUE,
        "TABLEAU_PROJECT_NAME": TABLEAU_PROJECT_NAME,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise ValueError(f"Tableau 업로드 설정이 부족합니다: {missing}")

    print("[Tableau Cloud 로그인 시작]")

    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=TABLEAU_TOKEN_NAME,
        personal_access_token=TABLEAU_TOKEN_VALUE,
        site_id=TABLEAU_SITE_ID,
    )

    server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        print("[Tableau Cloud 로그인 완료]")

        # 프로젝트 목록 조회
        all_projects, _ = server.projects.get()

        target_project = None

        for project in all_projects:
            if project.name == TABLEAU_PROJECT_NAME:
                target_project = project
                break

        if target_project is None:
            raise ValueError(f"Tableau 프로젝트를 찾을 수 없습니다: {TABLEAU_PROJECT_NAME}")

        datasource_item = TSC.DatasourceItem(project_id=target_project.id)
        datasource_item.name = "stock_daily"

        print("[Tableau Cloud 업로드 시작]")

        server.datasources.publish(
            datasource_item,
            hyper_file_path,
            mode=TSC.Server.PublishMode.Overwrite,
        )

        print("[Tableau Cloud 업로드 완료]")


# =========================
# 6. 메인 실행부
# =========================

def main() -> None:
    """
    전체 실행 순서:
    1. 주가 데이터 수집
    2. hyper 파일 생성
    3. 옵션에 따라 Tableau Cloud 업로드
    """

    print("=== 주가 데이터 자동화 시작 ===")

    stock_df = collect_all_stock_data()

    # 확인용 CSV도 함께 저장
    stock_df.to_csv("stock_daily_preview.csv", index=False, encoding="utf-8-sig")
    print("[CSV 미리보기 저장 완료] stock_daily_preview.csv")

    create_hyper_file(stock_df, HYPER_FILE_NAME)

    if PUBLISH_TO_TABLEAU:
        publish_hyper_to_tableau(HYPER_FILE_NAME)
    else:
        print("[Tableau 업로드 생략] .env의 PUBLISH_TO_TABLEAU=false 상태입니다.")

    print("=== 전체 작업 완료 ===")


if __name__ == "__main__":
    main()