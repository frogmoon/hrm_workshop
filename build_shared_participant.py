"""강사 준비가 완료된 공유 DB에서 독립 실행할 참가자 조회 노트북을 생성합니다."""

from pathlib import Path
import textwrap
import nbformat as nbf

ROOT = Path(__file__).resolve().parent


def md(text):
    """설명·역할·기대 결과를 작성합니다."""
    return nbf.v4.new_markdown_cell(textwrap.dedent(text).strip())


def code(text):
    """참가자 코드는 조회·assert·로컬 계산만 수행합니다."""
    c = nbf.v4.new_code_cell(textwrap.dedent(text).strip())
    c.metadata["execution_role"] = "parallel_read_or_local"
    return c


cells = [md('''
    # 공유 DB 참가자용 — Lab 1~5 조회·검증
    **🟢 모든 참가자 동시 실행 가능. 이 노트북에는 insert/update/delete/create 작업이 없습니다.**
    강사가 각 Lab의 준비·시연을 완료한 후 해당 섹션을 실행하세요.
    DB는 `hrm_workshop_lab1`~`hrm_workshop_lab5`이며 앞 Lab 코드를 실행할 필요가 없습니다.
    이 노트북은 강사가 적재한 데이터와 시연 결과를 읽습니다. 변경 시연은 원래 Lab 노트북에서 강사만 실행합니다.
    준비되지 않은 Lab은 결과가 없거나 assertion이 실패하므로 강사의 완료 안내 후 재실행하세요.
    가능하면 참가자는 DB read 권한 계정을 사용합니다. 코드 역할 표시 자체가 서버 권한을 대체하지 않습니다.
'''), code('''
    # 공통 준비: 모듈/인증을 읽습니다. 연결과 ping은 DB 데이터를 변경하지 않습니다.
    import sys
    from pathlib import Path
    from datetime import datetime, timedelta, timezone
    from IPython.display import display
    from bson import json_util
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "lab2/events.ejson").exists())
    sys.path.insert(0, str(root))
    from workshop_utils import connect, table, load_samples
    UTC = timezone.utc
'''), md('''
    ## Lab 1 🟢 모델·타입·최신 상태 확인
    **강사 준비:** 기기/이력/상태 적재, unique 및 이력 인덱스, 부분 갱신 시연.
    **참가자:** 문서 구조·키·타입·최신 A 상태를 확인하고 부분 갱신의 의미를 설명합니다.
'''), code('''
    # 샘플과 공유 DB를 읽어서 A의 최신 이벤트와 상태가 일치하는지 확인합니다.
    c, db = connect("hrm_workshop_lab1")
    try:
        assert db.devices.count_documents({}) == 3
        assert db.device_events.count_documents({}) == 18
        assert db.device_current_state.count_documents({}) == 12
        display(table(db.device_current_state.find({}, {"state.supportedModes": 0})))
        display(table(db.device_current_state.list_indexes()))
        latest = list(db.device_events.aggregate([
            {"$sort": {"eventTime": -1, "_id": -1}},
            {"$group": {"_id": {"deviceId": "$deviceId", "unitId": "$resource.unitId"}, "e": {"$first": "$$ROOT"}}}]))
        import json
        assert len(latest) == 6
        for row in latest:
            e = row["e"]
            s = db.device_current_state.find_one({"deviceId": e["deviceId"],
                "stateKey": json.dumps(["A", e["resource"]["unitId"]], separators=(",", ":"))})
            assert s["state"] == e["payload"] and s["lastEventId"] == e["_id"]
        # 누락·null·빈 배열은 서버 변경 없이 로컬 BSON JSON 예제로 비교합니다.
        display([{}, {"modes": None}, {"modes": []}])
        print("Lab 1: 최신 상태 6건 일치")
    finally:
        c.close()
'''), md('''
    ## Lab 2 🟢 OPEN·완료 시간·인덱스 검사량
    **강사 준비:** 기기/이벤트 적재, `device_type_resource_time` 인덱스 생성.
    **참가자:** 같은 데이터에서 OPEN/완료 구간을 구분하고 scan/index의 검사량을 비교합니다.
'''), code('''
    # 모든 참가자는 동일한 한국 시간 하루를 조회합니다.
    c, db = connect("hrm_workshop_lab2")
    try:
        start = datetime(2026, 10, 10, 15, tzinfo=UTC)
        end = start + timedelta(days=1)
        base = {"deviceId": "LAB2-D01", "resource.href": "/lab2/door/0", "eventTime": {"$gte": start, "$lt": end}}
        count = db.events.count_documents({**base, "eventType": "door_open"})
        result = list(db.events.aggregate([
            {"$match": {**base, "eventType": "door_close", "payload.openedAt": {"$gte": start, "$lt": end}}},
            {"$set": {"seconds": {"$divide": [{"$subtract": ["$eventTime", "$payload.openedAt"]}, 1000]}}},
            {"$group": {"_id": None, "completed": {"$sum": 1}, "total": {"$sum": "$seconds"}, "average": {"$avg": "$seconds"}}}]))
        assert count == 4 and result[0]["completed"] == 3 and result[0]["total"] == 300 and result[0]["average"] == 100
        display(table(result))
        query = {"find": "events", "filter": {**base, "eventType": "door_open"}, "sort": {"eventTime": -1}, "limit": 20}
        plans = []
        for label, hint in (("scan", {"$natural": 1}), ("index", "device_type_resource_time")):
            stats = db.command("explain", {**query, "hint": hint}, verbosity="executionStats")["executionStats"]
            plans.append({"strategy": label, **{k: stats[k] for k in ("nReturned", "totalDocsExamined", "totalKeysExamined")}})
        display(table(plans))
        assert plans[0]["nReturned"] == plans[1]["nReturned"] and plans[1]["totalDocsExamined"] < plans[0]["totalDocsExamined"]
    finally:
        c.close()
'''), md('''
    ## Lab 3 🟢 중복·순서 역전·timer 결과 확인
    **강사 준비/시연:** 원본 적재, 재전송, 조건부 상태 갱신, timer 2회, 늦은 CLOSE 해소.
    **참가자:** 이력 4,959건 유지, ORDER=CLOSED, 180/181초 알림 및 resolved 확인.
    참가자는 `process_event`/`timer_tick`을 호출하지 않습니다.
'''), code('''
    # 조회 결과로 강사 시나리오의 불변조건을 확인합니다.
    c, db = connect("hrm_workshop_lab3")
    try:
        assert db.events.count_documents({"datasetTag": "HRM_LAB2_DOOR_V1"}) == 4959
        state = db.current_state.find_one({"deviceId": "LAB3-ORDER"})
        assert state["doorState"] == "closed" and state["lastEventId"] == "lab3-order-close"
        alert_ids = ["LAB3-TIMER-179", "LAB3-TIMER-180", "LAB3-TIMER-181"]
        alerts = list(db.anomaly_results.find({"deviceId": {"$in": alert_ids}}))
        assert {a["deviceId"] for a in alerts} == {"LAB3-TIMER-180", "LAB3-TIMER-181"}
        assert next(a for a in alerts if a["deviceId"] == "LAB3-TIMER-181")["status"] == "resolved"
        display(table(alerts))
        display(table(db.current_state.find({"deviceId": {"$in": alert_ids}})))
    finally:
        c.close()
'''), md('''
    ## Lab 4 🟢 일반/Time Series 동일 집계
    **강사 준비:** 일반/TS 컬렉션 생성·적재, 보조 인덱스. TTL은 강사 선택 시연.
    **참가자:** 측정 건수·options·집계 일치 확인. insertMany·TTL insert를 재실행하지 않습니다.
'''), code('''
    # 집계만 실행합니다. 초기 적재가 완료되어 있어야 합니다.
    c, db = connect("hrm_workshop_lab4")
    try:
        assert db.door_duration_v1_ts.count_documents({}) == 799
        assert db.temperature_v1_ts.count_documents({}) == 3456
        display(db.temperature_v1_ts.options())
        pipeline = [{"$match": {"meta.deviceId": "LAB2-D01"}},
            {"$group": {"_id": {"$dateTrunc": {"date": "$eventTime", "unit": "hour", "timezone": "UTC"}},
                        "count": {"$sum": 1}, "avgC": {"$avg": "$temperatureC"}}}, {"$sort": {"_id": 1}}]
        regular = list(db.temperature_v1_regular.aggregate(pipeline))
        ts = list(db.temperature_v1_ts.aggregate(pipeline))
        assert regular == ts and sum(r["count"] for r in ts) == 144
        display(table(ts))
        # 선택 TTL 시연 컬렉션의 존재/건수만 확인합니다. 측정값을 추가하지 않습니다.
        if "ttl_demo" in db.list_collection_names():
            print("TTL 시연 현재 건수:", db.ttl_demo.count_documents({}))
    finally:
        c.close()
'''), md('''
    ## Lab 5 🟢 검색·필터·근거 확인
    **강사 준비:** 매뉴얼 12건, 임베딩 갱신, text/vector 인덱스.
    **참가자:** 코드에 임베딩 적재 없이 기존 문서 벡터로 Vector Search 동작을 확인합니다.
    이 섹션의 검색은 **문서 유사도 확인**이며 자연어 질의 임베딩·RAG 품질 평가는 원래 Lab 5를 참고하세요.
    자연어 모델을 로컬에서 로드하고 질문만 encode하는 작업은 동시 실행 가능하지만 DB bulk_write는 강사 작업입니다.
    autoEmbed/rerank 확장은 강사 선택 시연입니다. API 호출에 따라 추가 비용/레이트 제한이 발생할 수 있습니다.
'''), code('''
    # DB에서 읽은 문서 벡터를 질의에 사용합니다. embedding을 생성/업데이트하지 않습니다.
    c, db = connect("hrm_workshop_lab5")
    try:
        assert db.manual_chunks.count_documents({}) == 12
        seed = db.manual_chunks.find_one({"_id": "M1-DRAIN"})
        assert len(seed["embedding"]) == 384
        scope = {"modelCode": "LAB2-MODEL-1", "countryCode": "KR", "language": "ko"}
        result = list(db.manual_chunks.aggregate([
            {"$vectorSearch": {"index": "manual_vector", "path": "embedding", "queryVector": seed["embedding"],
                               "filter": scope, "exact": True, "limit": 4}},
            {"$set": {"score": {"$meta": "vectorSearchScore"}}}, {"$unset": "embedding"}]))
        assert result and result[0]["_id"] == "M1-DRAIN"
        assert all(all(d[k] == v for k, v in scope.items()) for d in result)
        display(table(result)[["_id", "title", "modelCode", "score", "source", "page"]])
        lexical = list(db.manual_chunks.aggregate([
            {"$search": {"index": "manual_text_ko", "compound": {
                "must": [{"text": {"query": "배수 호스", "path": ["title", "text"]}}],
                "filter": [{"equals": {"path": k, "value": v}} for k, v in scope.items()]}}},
            {"$limit": 3}, {"$project": {"title": 1, "text": 1, "source": 1, "page": 1}}]))
        assert lexical
        display(table(lexical))
    finally:
        c.close()
''')]
notebook = nbf.v4.new_notebook(cells=cells, metadata={
    "kernelspec": {"display_name": "HRM Workshop (Python)", "language": "python", "name": "hrm-workshop"},
    "language_info": {"name": "python"}})
nbf.validate(notebook)
nbf.write(notebook, ROOT / "Shared_DB_Participant.ipynb")
print("공유 DB 참가자 조회 노트북 생성 완료")
