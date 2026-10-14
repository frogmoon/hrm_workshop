"""기존 Lab 3·4·5를 변경하지 않고 Lab 1·2 교육용 노트북을 생성합니다."""

from pathlib import Path
import textwrap
import sys
import nbformat as nbf

ROOT = Path(__file__).resolve().parent


def md(source):
    """한국어 진행 안내를 Markdown 셀로 만듭니다."""
    return nbf.v4.new_markdown_cell(textwrap.dedent(source).strip())


def code(source):
    """실행 코드를 주석과 함께 코드 셀로 만듭니다."""
    return nbf.v4.new_code_cell(textwrap.dedent(source).strip())


def save(lab, filename, cells):
    """기존 노트북과 동일한 프로젝트 커널을 지정합니다."""
    if len(sys.argv) > 1 and sys.argv[1] != lab:
        return
    notebook = nbf.v4.new_notebook(cells=cells, metadata={
        "kernelspec": {"display_name": "HRM Workshop (Python)", "language": "python", "name": "hrm-workshop"},
        "language_info": {"name": "python"}})
    nbf.validate(notebook)
    nbf.write(notebook, ROOT / lab / filename)


SETUP = '''
    # 프로젝트 공통 모듈을 불러옵니다. 인증정보는 로컬 파일/환경변수/숨김 입력에서 읽습니다.
    import sys
    from pathlib import Path
    from datetime import datetime, timedelta, timezone
    from copy import deepcopy
    from collections import Counter
    from IPython.display import display
    from bson import json_util
    import pandas as pd
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "lab2/events.ejson").exists())
    sys.path.insert(0, str(root))
    from workshop_utils import connect, load_samples, insert_once, table
    UTC = timezone.utc
'''

save("lab1", "Lab1_Data_Modeling.ipynb", [
md('''
    # Lab 1 — HRM 데이터 모델링 (60분)
    **목표:** 기기 마스터·원본 이력·최신 상태의 역할을 구분하고 Entity별 복합키를 보존합니다.
    기존 lab1의 기기 3건, 이벤트 18건, 상태 12건을 사용합니다. 이벤트는 Entity A만 포함하며
    상태의 B~F는 별도 교육용 구조 예시입니다. anomaly_results는 빈 배열입니다.
    UUID 문자열과 ObjectId를 임의로 같은 타입으로 바꾸지 않습니다. MODE 코드를 문 열림 의미로 추정하지 않습니다.

    위에서 아래로 실행하세요. 전용 DB `hrm_workshop_lab1`을 사용합니다.
    흐름: 구조·타입(10분) → 키·조회(15분) → 최신 상태(10분) → 부분 갱신(15분) → 모델 비교(10분).
'''), code(SETUP + '''
    # Extended JSON을 BSON 타입에 대응하는 Python 값으로 읽습니다.
    samples = {p.stem: json_util.loads(p.read_text(), json_options=json_util.JSONOptions(tz_aware=True))
               for p in (root / "lab1").glob("*.ejson")}
    assert {k: len(v) for k, v in samples.items()} == {
        "devices": 3, "device_events": 18, "device_current_state": 12, "anomaly_results": 0}
    client, db = connect("hrm_workshop_lab1")
    for name, documents in samples.items():
        insert_once(db[name], documents)
    # 빈 anomaly_results도 역할을 보여주기 위해 일반 컬렉션으로 생성합니다.
    if "anomaly_results" not in db.list_collection_names():
        db.create_collection("anomaly_results")
    display(table([{"collection": name, "sampleCount": len(docs), "storedCount": db[name].count_documents({})}
                   for name, docs in samples.items()]))
    display(table(samples["device_events"][:1]))
'''),
md('''
    ## 1. 역할과 타입
    | 컬렉션 | 역할 | 대표 접근 |
    |---|---|---|
    | devices | 상대적으로 안정적인 기기 정보 | 기기 ID 조회 |
    | device_events | 변경 이력 | 기기·리소스·기간 조회 |
    | device_current_state | Entity별 최신 상태 | deviceId + stateKey 조회 |
    | anomaly_results | 규칙 평가 결과 | 상태·감지 시각 조회 (Lab 3) |

    무한히 늘어나는 이력 배열은 기기 문서에 계속 붙이지 않습니다.
    bounded modes 배열은 상태 안에 보관하고 모델 정보를 이력에 함께 저장하면 당시 모델의 관측값을 보존할 수 있습니다.
'''), code('''
    # 동일 필드의 BSON 타입을 서버 집계로 검증합니다.
    for name, field in (("devices", "_id"), ("device_events", "eventTime"),
                        ("device_current_state", "_id"), ("device_current_state", "state.modes")):
        result = list(db[name].aggregate([
            {"$group": {"_id": {"$type": "$" + field}, "count": {"$sum": 1}}}, {"$sort": {"_id": 1}}]))
        print(name, field)
        display(table(result))
    assert all(isinstance(e["eventTime"], datetime) for e in samples["device_events"])
'''),
md('''
    ## 2. Entity별 원래 복합키를 stateKey로 표현
    A/D: unitId, B: unitId+component+filterName, C: subDevice,
    E: unitId+id, F: unitId+target입니다. 이 샘플에는 G/H가 없습니다.
    stateKey는 정해진 순서의 JSON 배열 문자열이며 식별자의 자료형·순서를 보존합니다.
'''), code('''
    # 키 계약에 따라 stateKey를 재계산하고 제공된 샘플과 비교합니다.
    import json
    key_fields = {"A": ["unitId"], "B": ["unitId", "component", "filterName"],
                  "C": ["subDevice"], "D": ["unitId"], "E": ["unitId", "id"], "F": ["unitId", "target"]}
    for state in samples["device_current_state"]:
        values = [state["entity"], *[state["key"][f] for f in key_fields[state["entity"]]]]
        assert state["stateKey"] == json.dumps(values, separators=(",", ":"))
    states = db.device_current_state
    states.create_index([("deviceId", 1), ("stateKey", 1)], unique=True, name="device_state_unique")
    D = samples["devices"][0]["_id"]
    identity = {"deviceId": D, "stateKey": '["A","0"]'}
    display(states.find_one(identity))
    display(table(states.find({"entity": "A", "state.modes": "MODE_01"}, {"state.supportedModes": 0})))
    assert states.count_documents({"entity": "A", "state.modes": "MODE_01"}) == 6
'''),
md('''
    ## 3. 원본 이력에서 최신 상태 계산
    A의 full snapshot 이벤트만 대상으로 기기+unitId별 마지막 값을 구합니다.
    부분 payload나 동일 eventTime의 충돌은 다른 계약이 필요하며 여기서는 full snapshot을 전제로 합니다.
'''), code('''
    # sort 후 group의 first로 리소스별 최신 이벤트를 선택합니다.
    history = db.device_events
    history.create_index([("deviceId", 1), ("resource.href", 1), ("eventTime", -1)], name="device_resource_time")
    latest = list(history.aggregate([
        {"$sort": {"eventTime": -1, "_id": -1}},
        {"$group": {"_id": {"deviceId": "$deviceId", "unitId": "$resource.unitId"}, "event": {"$first": "$$ROOT"}}}
    ]))
    assert len(latest) == 6
    checks = []
    for item in latest:
        event = item["event"]
        key = json.dumps(["A", event["resource"]["unitId"]], separators=(",", ":"))
        state = states.find_one({"deviceId": event["deviceId"], "stateKey": key})
        assert state["lastEventId"] == event["_id"] and state["state"] == event["payload"]
        assert state["updatedAt"] == event["eventTime"]
        checks.append({"deviceId": event["deviceId"], "stateKey": key, "latestEventId": event["_id"], "match": True})
    display(table(checks))
'''),
md('''
    ## 4. 부분 갱신과 누락·null·빈 배열
    `$set: {"state.modes": ...}`는 형제 필드를 유지합니다. `$set: {"state": ...}`는 state 전체를 교체합니다.
    누락은 "변경하지 않음", null은 "값 없음", []는 "빈 목록"으로 이 실습에서 정의합니다.
    업무 계약마다 다를 수 있으므로 수집 시스템과 합의해야 합니다.
    실습은 원본 상태를 복사한 `modeling_demo` 컬렉션에서 수행합니다.
'''), code('''
    # 복사한 한 문서에서 연산 차이를 실제로 검증합니다. 원본 상태 컬렉션은 수정하지 않습니다.
    original = states.find_one(identity)
    demo = db.modeling_demo
    working = deepcopy(original)
    working["_id"] = "lab1-partial-update"
    demo.replace_one({"_id": working["_id"]}, working, upsert=True)
    result = demo.update_one({"_id": working["_id"]}, {"$set": {"state.modes": ["LAB1-DEMO"]}}, upsert=False)
    assert result.matched_count == 1
    assert demo.find_one({"_id": working["_id"]})["state"]["supportedModes"] == original["state"]["supportedModes"]
    # 없는 문서는 UPDATE만으로 생성하지 않습니다.
    assert demo.update_one({"_id": "not-present"}, {"$set": {"state.modes": []}}, upsert=False).matched_count == 0
    observed = []
    for label, change in (("missing", {"$unset": {"state.modes": ""}}),
                          ("null", {"$set": {"state.modes": None}}),
                          ("empty array", {"$set": {"state.modes": []}})):
        demo.update_one({"_id": working["_id"]}, change)
        row = list(demo.aggregate([{ "$match": {"_id": working["_id"]}},
                                  {"$project": {"_id": 0, "type": {"$type": "$state.modes"}}}]))[0]
        observed.append({"case": label, "bsonType": row["type"]})
    assert [r["bsonType"] for r in observed] == ["missing", "null", "array"]
    display(table(observed))
    # 다음 실행도 같은 기준에서 시작하도록 복사본을 원래 값으로 되돌립니다.
    demo.replace_one({"_id": working["_id"]}, working)
'''),
code('''
    # 필요한 기기 메타데이터만 lookup하고 문서 크기를 측정합니다.
    joined = list(states.aggregate([
        {"$match": {"deviceId": D}},
        {"$lookup": {"from": "devices", "localField": "deviceId", "foreignField": "_id", "as": "device"}},
        {"$project": {"stateKey": 1, "entity": 1, "modelCode": {"$first": "$device.product.modelCode"},
                      "bsonBytes": {"$bsonSize": "$$ROOT"}}}
    ]))
    display(table(joined))
    assert all(r["modelCode"] == samples["devices"][0]["product"]["modelCode"] for r in joined)
    client.close()  # 단일 노트북에서 재사용한 연결을 종료합니다.
'''), md('''
    ## 리뷰
    - 자주 함께 조회되는 기기 메타데이터는 내장할까요, 참조할까요?
    - 이력과 최신 상태의 쓰기·보존·조회 패턴이 다른 이유를 설명하세요.
    - unique 키 충돌 시 원본 키 계약과 기존 중복을 먼저 확인하세요.
    - 이벤트/상태 간 여러 문서 쓰기의 원자성·재시작 처리는 Lab 3에서 다룹니다.

    [데이터 모델링](https://www.mongodb.com/docs/manual/data-modeling/) ·
    [부분 갱신](https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/)
''')])

save("lab2", "Lab2_Aggregation_and_Indexes.ipynb", [
md('''
    # Lab 2 — Aggregation과 인덱스 (40분)
    기기 24대·mode_snapshot 3,360건·door_open 800건·door_close 799건의 합성 데이터를 사용합니다.
    OPEN 횟수와 완료 구간 수를 구분하고 미완료 OPEN은 duration 평균에서 제외합니다.
    MODE 코드는 의미 미확정 값이므로 door_open/door_close와 혼동하지 않습니다.
    전용 DB `hrm_workshop_lab2`에서 적재(5분), 최신 조회·일별 분석(10분),
    완료 시간·모델 분석(15분), explain 비교·리뷰(10분) 순으로 진행합니다.
'''), code(SETUP + '''
    # 같은 파일을 재실행해도 결정적 _id 기준 원본 건수는 유지됩니다.
    events, devices = load_samples()
    client, db = connect("hrm_workshop_lab2")
    insert_once(db.events, events)
    insert_once(db.devices, devices)
    history = db.events
    assert history.count_documents({}) == 4959 and db.devices.count_documents({}) == 24
    assert dict(Counter(e["eventType"] for e in events)) == {"mode_snapshot": 3360, "door_open": 800, "door_close": 799}
    D = "LAB2-D01"
    # 기간은 한국 시간 10월 11일 하루입니다. UTC로 변환한 시작 포함·끝 제외를 사용합니다.
    START = datetime(2026, 10, 10, 15, tzinfo=UTC)
    END = START + timedelta(days=1)
    display(table(events[:2]))
'''), md("## 1. 리소스별 최신 이벤트\n하나의 기기에서 모드와 도어가 서로 다른 리소스이므로 도어 리소스를 지정합니다."),
code('''
    # 마지막 도어 이벤트가 미완료 OPEN인지 원본 이력으로 확인합니다.
    latest_filter = {"deviceId": D, "resource.href": "/lab2/door/0"}
    latest = list(history.find(latest_filter).sort([("eventTime", -1), ("_id", -1)]).limit(1))
    assert latest[0]["_id"] == "LAB2-D01-open-unfinished" and latest[0]["payload"]["doorState"] == "open"
    display(table(latest))
'''), md("## 2. 한국 시간의 일별·시간대별 OPEN 횟수\n동일 기간의 Python 기준 계산과 MongoDB 집계를 비교합니다."),
code('''
    # eventTime은 UTC BSON Date로 저장하고 일별 그룹에서만 한국 시간대를 적용합니다.
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
    open_filter = {"deviceId": D, "eventType": "door_open", "resource.href": "/lab2/door/0",
                   "eventTime": {"$gte": START, "$lt": END}}
    daily_pipeline = [{"$match": open_filter}, {"$group": {
        "_id": {"$dateToString": {"date": "$eventTime", "format": "%Y-%m-%d", "timezone": "Asia/Seoul"}},
        "openCount": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
    daily = list(history.aggregate(daily_pipeline))
    selected = [e for e in events if e["deviceId"] == D and e["eventType"] == "door_open" and START <= e["eventTime"] < END]
    expected = Counter(e["eventTime"].astimezone(KST).strftime("%Y-%m-%d") for e in selected)
    assert {r["_id"]: r["openCount"] for r in daily} == dict(expected)
    assert sum(r["openCount"] for r in daily) == 4
    display(table(daily))
    # 날짜와 시간을 같이 사용하여 서로 다른 날짜의 같은 시각을 잘못 합치지 않습니다.
    hourly = [{"$match": open_filter}, {"$group": {
        "_id": {"$dateTrunc": {"date": "$eventTime", "unit": "hour", "timezone": "Asia/Seoul"}},
        "openCount": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
    display(table(history.aggregate(hourly)))
'''), md('''
    ## 3. 완료 구간의 합계·평균·최대
    시작/종료가 모두 해당 기간 안에 있는 구간을 대상으로 합니다. 기간을 가로지르는 구간은 이 정의에서는 제외합니다.
    elapsed 시간은 실제 Date 차이로 계산하며 미완료 구간을 0초로 넣지 않습니다.
'''), code('''
    # CLOSE가 참조하는 OPEN과 동일 기기/리소스/시각인지 먼저 검증합니다.
    by_id = {e["_id"]: e for e in events}
    for e in events:
        if e["eventType"] == "door_close":
            o = by_id[e["payload"]["openEventId"]]
            assert o["deviceId"] == e["deviceId"] and o["resource"] == e["resource"]
            assert o["eventTime"] == e["payload"]["openedAt"] <= e["eventTime"]
    duration_pipeline = [
        {"$match": {"deviceId": D, "eventType": "door_close", "resource.href": "/lab2/door/0",
                    "eventTime": {"$gte": START, "$lt": END}, "payload.openedAt": {"$gte": START, "$lt": END}}},
        {"$set": {"durationSeconds": {"$divide": [{"$subtract": ["$eventTime", "$payload.openedAt"]}, 1000]}}},
        {"$group": {"_id": None, "closedCycles": {"$sum": 1}, "totalSeconds": {"$sum": "$durationSeconds"},
                    "avgSeconds": {"$avg": "$durationSeconds"}, "maxSeconds": {"$max": "$durationSeconds"}}}
    ]
    durations = list(history.aggregate(duration_pipeline))
    assert durations == [{"_id": None, "closedCycles": 3, "totalSeconds": 300.0, "avgSeconds": 100.0, "maxSeconds": 240.0}]
    display(table(durations))
    print("OPEN 4회 / 완료 3구간 / 합계 300초 / 평균 100초 / 미완료 1구간")
'''), md('''
    ## 4. 모델별 코호트 — 이벤트가 없는 기기도 포함
    devices를 시작점으로 lookup하여 OPEN이 없는 기기를 0회로 유지합니다.
    fullyObserved/observedDays는 fixture의 관측 계약이며 이벤트가 없다는 사실만으로 관측 완료를 추정하지 않습니다.
'''), code('''
    # 기기 마스터가 분모입니다. 이벤트를 먼저 group하면 0회 기기가 빠질 수 있습니다.
    cohort_pipeline = [
        {"$match": {"lab2.fullyObserved": True, "lab2.observedDays": 7}},
        {"$lookup": {"from": "events", "let": {"d": "$_id"}, "pipeline": [
            {"$match": {"eventType": "door_open", "eventTime": {"$gte": START, "$lt": END},
                        "$expr": {"$eq": ["$deviceId", "$$d"]}}}, {"$count": "n"}], "as": "openCounts"}},
        {"$set": {"openCount": {"$ifNull": [{"$first": "$openCounts.n"}, 0]}}},
        {"$group": {"_id": "$product.modelCode", "deviceCount": {"$sum": 1},
                    "totalOpens": {"$sum": "$openCount"}, "avgOpensPerDevice": {"$avg": "$openCount"}}},
        {"$sort": {"_id": 1}}
    ]
    cohort = list(db.devices.aggregate(cohort_pipeline))
    expected_counts = Counter(e["deviceId"] for e in events if e["eventType"] == "door_open" and START <= e["eventTime"] < END)
    for row in cohort:
        members = [d for d in devices if d["product"]["modelCode"] == row["_id"]]
        assert row["deviceCount"] == len(members)
        assert row["totalOpens"] == sum(expected_counts[d["_id"]] for d in members)
    assert sum(r["deviceCount"] for r in cohort) == 24
    display(table(cohort))
'''), md('''
    ## 5. explain — 같은 쿼리의 검사량 비교
    이 fixture의 같은 조건·같은 결과를 COLLSCAN과 인덱스로 비교합니다.
    재실행에도 `$natural` 힌트로 스캔 기준을 고정합니다.
    작은 샘플의 시간 차이를 운영 성능으로 일반화하지 않고 docsExamined/keysExamined를 확인합니다.
'''), code('''
    # 재실행에도 같은 기준으로 비교하도록 전체 스캔은 $natural 힌트로 측정합니다.
    query = {"find": history.name, "filter": open_filter,
             "sort": {"eventTime": -1}, "limit": 20}
    scan = db.command("explain", {**query, "hint": {"$natural": 1}}, verbosity="executionStats")
    history.create_index([("deviceId", 1), ("eventType", 1), ("resource.href", 1), ("eventTime", -1)],
                         name="device_type_resource_time")
    indexed = db.command("explain", {**query, "hint": "device_type_resource_time"}, verbosity="executionStats")
    rows = []
    for label, plan in (("COLLSCAN baseline", scan), ("indexed", indexed)):
        stats = plan["executionStats"]
        rows.append({"strategy": label, **{k: stats[k] for k in
                    ("nReturned", "totalDocsExamined", "totalKeysExamined", "executionTimeMillis")}})
    assert rows[0]["nReturned"] == rows[1]["nReturned"] == 4
    assert rows[1]["totalDocsExamined"] < rows[0]["totalDocsExamined"]
    display(table(rows))
    # 실제 반환 문서도 일치하는지 확인합니다. explain은 결과 문서를 반환하지 않습니다.
    a = list(history.find(open_filter).sort("eventTime", -1).limit(20).hint([("$natural", 1)]))
    b = list(history.find(open_filter).sort("eventTime", -1).limit(20).hint("device_type_resource_time"))
    assert a == b
    client.close()  # 노트북 종료 시 연결을 정리합니다.
'''), md('''
    ## 리뷰
    - OPEN 4건과 완료 3구간의 차이를 설명하세요.
    - 하루의 timezone/기간 경계 및 기간을 가로지르는 구간 정책을 정하세요.
    - 중복·순서 역전·미완료 감지는 Lab 3에서, Time Series 비교는 Lab 4에서 확장합니다.
    - 인덱스는 조회 패턴의 등호 조건 뒤 정렬/범위 조건을 기준으로 선택하고 쓰기 비용도 비교하세요.

    [Aggregation](https://www.mongodb.com/docs/manual/aggregation/) ·
    [Explain](https://www.mongodb.com/docs/manual/tutorial/analyze-query-plan/)
''')])
print("Lab 1·2 노트북 생성 완료")
