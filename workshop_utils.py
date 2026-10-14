"""HRM 노트북의 공통 입출력·연결 기능. 업무 로직은 각 노트북에 있습니다."""

import os
import time
from pathlib import Path

import pandas as pd
from bson import json_util
from pymongo import MongoClient
from pymongo.operations import UpdateOne, SearchIndexModel


def repository_root():
    """노트북이 lab3/4/5 또는 저장소 루트에서 실행되어도 lab2를 찾습니다."""
    for candidate in (Path.cwd(), *Path.cwd().parents):
        if (candidate / "lab2/events.ejson").is_file():
            return candidate
    raise FileNotFoundError("hrm_workshop 저장소 안에서 노트북을 실행하세요.")


def load_samples():
    """Extended JSON의 $date를 UTC BSON Date에 대응하는 datetime으로 변환합니다."""
    root = repository_root()
    events = json_util.loads((root / "lab2/events.ejson").read_text(),
                             json_options=json_util.JSONOptions(tz_aware=True))
    devices = json_util.loads((root / "lab2/devices.ejson").read_text())
    return events, devices


def connect(database_name):
    """한 노트북에서 한 MongoClient를 재사용합니다. URI는 노트북에 저장하지 않습니다."""
    # 환경변수를 우선 사용하고, 이 컴퓨터에서는 이전에 저장한 별도 인증 파일을 읽습니다.
    uri = os.environ.get("MDB_MCP_CONNECTION_STRING", "").strip()
    secret = Path.home() / ".config/opencode/mongodb-workshop-uri"
    if not uri and secret.exists():
        uri = secret.read_text().strip()
    if not uri:
        # 다른 참가자는 숨김 입력으로 URI를 입력할 수 있으며 출력에는 남기지 않습니다.
        from getpass import getpass
        uri = getpass("MongoDB URI: ").strip()
    # 순차 실행 교육용 워크로드이므로 풀 크기는 드라이버 기본값을 사용합니다.
    client = MongoClient(uri, appname="hrm-hands-on-notebooks", tz_aware=True)
    client.admin.command("ping")
    print("연결 성공 / MongoDB", client.server_info()["version"], "/ DB", database_name)
    return client, client[database_name]


def insert_once(collection, documents):
    """일반 컬렉션에서 결정적 _id를 사용해 재실행 시 원본을 중복 적재하지 않습니다."""
    if not documents:
        return
    operations = [UpdateOne({"_id": d["_id"]},
                            {"$setOnInsert": {k: v for k, v in d.items() if k != "_id"}},
                            upsert=True) for d in documents]
    # 서버로 보내는 호출 수를 줄이기 위해 500건 단위로 배치합니다.
    for start in range(0, len(operations), 500):
        collection.bulk_write(operations[start:start + 500], ordered=False)


def table(documents):
    """중첩 문서를 표로 평탄화합니다. 표본 표시에는 호출 측에서 limit을 적용합니다."""
    return pd.json_normalize(list(documents))


def ensure_search_index(collection, name, definition, index_type="search"):
    """검색 인덱스는 비동기로 생성되므로 queryable 상태까지 제한 시간 동안 기다립니다."""
    existing = list(collection.list_search_indexes(name))
    if not existing:
        collection.create_search_index(SearchIndexModel(
            name=name, definition=definition, type=index_type))
    elif existing[0].get("latestDefinition") != definition:
        raise RuntimeError(f"{name}: 기존 정의가 다릅니다. 새 이름으로 생성하세요.")
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        indexes = list(collection.list_search_indexes(name))
        if indexes and indexes[0].get("status") == "FAILED":
            raise RuntimeError(f"{name}: 인덱스 빌드 실패. Atlas 상태를 확인하세요.")
        if indexes and indexes[0].get("queryable"):
            print(name, "queryable")
            return
        time.sleep(5)
    raise TimeoutError(f"{name}: 아직 빌드 중입니다. Atlas에서 확인 후 셀을 재실행하세요.")
