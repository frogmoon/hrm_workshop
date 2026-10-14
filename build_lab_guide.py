"""샘플 데이터 분석과 전체 워크플로우를 로컬 읽기 전용 가이드로 생성합니다."""

from pathlib import Path
import textwrap
import nbformat as nbf

ROOT = Path(__file__).resolve().parent


def md(text):
    """진행·해석·역할 안내를 Markdown으로 제공합니다."""
    return nbf.v4.new_markdown_cell(textwrap.dedent(text).strip())


def code(text):
    """파일 조회와 로컬 계산만 수행하는 셀을 생성합니다."""
    cell = nbf.v4.new_code_cell(textwrap.dedent(text).strip())
    cell.metadata["execution_role"] = "parallel_read_or_local"
    return cell


cells = [md('''
    # HRM Hands-on Lab Guide — 데이터 분석과 전체 실습 워크플로우

    이 노트북은 **첫 번째로 읽을 전체 실습 지도**입니다. 원본 EJSON을 직접 분석하여 구조·건수·기간·관계를
    확인하고, Lab 1~5에서 무엇을 입력하고 어떤 결과를 확인하는지 설명합니다.
    **🟢 모든 참가자 동시 실행 가능: 로컬 파일 읽기·계산만 수행하며 MongoDB 연결/쓰기 작업은 없습니다.**

    장표의 데이터 모델링 → Aggregation/인덱스 → 이벤트 품질 → Time Series → Vector Search/RAG 흐름을 따릅니다.
    실제 구현/기대 결과는 저장소 fixture로 검증했습니다. 고객 운영 데이터나 성능 측정 결과가 아닙니다.
    온도와 매뉴얼은 교육용 합성 자료이며 기본 RAG 출력은 검색 근거·프롬프트이지 LLM 생성 답변이 아닙니다.

    ## 빠른 시작
    1. 이 가이드의 데이터 분석 셀을 Run All로 확인합니다.
    2. 강사가 해당 Lab의 🔴 준비·시연 작업을 완료합니다.
    3. 참가자는 [Shared_DB_Participant.ipynb](Shared_DB_Participant.ipynb)의 해당 Lab을 실행합니다.
    4. 원래 Lab 노트북은 코드 설명·강사 시연 결과를 함께 읽는 용도로 사용합니다.
'''), code('''
    # 저장소 안 어느 폴더에서 실행해도 샘플 경로를 찾습니다. 인증정보는 필요하지 않습니다.
    from pathlib import Path
    from collections import Counter
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo
    import json
    import pandas as pd
    from bson import json_util
    from IPython.display import display
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "lab2/events.ejson").exists())
    UTC, KST = timezone.utc, ZoneInfo("Asia/Seoul")
    def read_ejson(path):
        # $date와 $oid를 실제 datetime/ObjectId로 읽어 문자열 집계와 구분합니다.
        return json_util.loads(path.read_text(), json_options=json_util.JSONOptions(tz_aware=True))
    lab1 = {p.stem: read_ejson(p) for p in (root / "lab1").glob("*.ejson")}
    events, devices = read_ejson(root / "lab2/events.ejson"), read_ejson(root / "lab2/devices.ejson")
    display(pd.DataFrame([{"lab": "Lab 1", "file": k + ".ejson", "documents": len(v)} for k, v in lab1.items()] +
                        [{"lab": "Lab 2", "file": "devices.ejson", "documents": len(devices)},
                         {"lab": "Lab 2", "file": "events.ejson", "documents": len(events)}]))
'''), md('''
    ## 1. 데이터 세트의 역할과 구분

    | 구분 | 자료 | 의미 |
    |---|---|---|
    | Lab 1 | 기기 3건 / A 이력 18건 / 상태 12건 / 빈 이상 결과 | Entity별 키·상태 모델 실습 |
    | Lab 2 | 기기 24대 / 이벤트 4,959건 | 일별 문 열림·완료 시간·모델 코호트 실습 |
    | Lab 3 | Lab 2 원본 + SYNTHETIC_LAB3 시나리오 | 재전송·순서 역전·179/180/181초 경계 |
    | Lab 4 | 원본 CLOSE에서 만든 구간 799건 + 합성 온도 3,456건 | 일반/Time Series 결과 비교 |
    | Lab 5 | 합성 매뉴얼 chunk 12건 | 한국어 검색·다국어 벡터·모델 필터·근거 인용 |

    Lab 1의 UUID 기기 3대와 Lab 2의 `LAB2-Dxx` 기기 24대는 **서로 다른 fixture**입니다.
    한 기기의 이력이 Lab 1→Lab 2로 이어진다고 해석하지 않습니다.
    Lab 2→Lab 3/4는 이벤트 구조를 재사용하고 Lab 5는 모델 코드만 검색 범위로 이어갑니다.
'''), md('''
    ## 2. Lab 1 — 기기·이력·최신 상태 모델
    devices는 기기 마스터, device_events는 시간순 이력, device_current_state는 Entity별 최신 값입니다.
    `anomaly_results=[]`는 아직 감지 결과가 없다는 fixture 정의이며 감지 기능 실행 결과가 아닙니다.
    이벤트 18건은 A 리소스 2개(unit 0/1), 3기기, 각 3회 snapshot입니다.
    상태 12건 중 A 6건은 이력과 비교할 수 있고 B~F 6건은 추가 구조 예시입니다. G/H fixture는 없습니다.
'''), code('''
    # Entity별 키/필드와 이력 리소스를 실제 파일에서 확인합니다.
    states = lab1["device_current_state"]
    display(pd.DataFrame([{"entity": s["entity"], "deviceId": s["deviceId"], "key": s["key"],
                           "stateKey": s["stateKey"], "stateFields": list(s["state"])} for s in states]))
    display(pd.DataFrame(Counter(e["resource"]["href"] for e in lab1["device_events"]).items(),
                         columns=["resource", "events"]))
    assert len(states) == 12 and Counter(s["entity"] for s in states)["A"] == 6
    latest = {}
    for e in sorted(lab1["device_events"], key=lambda e: (e["eventTime"], e["_id"])):
        latest[(e["deviceId"], e["resource"]["unitId"])] = e
    for s in states:
        if s["entity"] == "A":
            e = latest[(s["deviceId"], s["key"]["unitId"])]
            assert s["lastEventId"] == e["_id"] and s["state"] == e["payload"]
    print("A 최신 상태 6건과 이력 snapshot 일치")
'''), md('''
    ### Lab 1의 필드 계약
    - `deviceId`: UUID 모양의 **문자열**. 상태 `_id`의 ObjectId와 구분합니다.
    - `stateKey`: `[entity, key values...]` JSON 배열 문자열. 키 순서·타입을 고정합니다.
    - A/D unitId, B unitId+component+filterName, C subDevice, E unitId+id, F unitId+target.
    - `state.modes`·`supportedModes`: bounded 배열입니다. MODE 의미를 door 상태로 추정하지 않습니다.
    - 원본 이력을 무제한 배열로 최신 상태 문서에 붙이지 않고 별도 컬렉션으로 보관합니다.
    - 부분 `$set state.modes`는 형제 필드 보존, `$set state`는 전체 객체 교체입니다.
    - 누락/null/[] 의미는 업무 계약입니다. 실습에서는 변경 없음/값 없음/빈 목록을 구분합니다.
'''), md('''
    ## 3. Lab 2 — 이벤트 구조 분석

    | 필드 | 설명 | 실습 활용 |
    |---|---|---|
    | `_id` | 재전송에도 유지하는 이벤트 식별자 | 멱등성·원본 추적 |
    | `deviceId`, `locationId` | 장치·위치 식별자 | 기기 마스터 연결 |
    | `resource.href/unitId/component` | 리소스 식별 | 다른 리소스 상태를 섞지 않기 |
    | `eventType` | mode_snapshot / door_open / door_close | 의미별 필터 |
    | `eventTime` | 발생 시각 BSON Date | 집계·최신성·시간 범위 |
    | `receivedAt` | 수신 시각 BSON Date | 도착 지연 확인 |
    | `context` | 당시 모델·국가·지역·시간대 | 이벤트 시점 메타데이터 |
    | `payload` | 모드 배열 또는 도어 상태 | 이벤트 종류별 값 |
    | `datasetTag` | HRM_LAB2_DOOR_V1 | fixture 구분 |

    door_close의 `payload.openEventId`는 OPEN을 연결하고 `openedAt`은 시작 시각입니다.
    MODE 스냅샷은 지원/동작 모드 예시이지 문 열림 신호가 아닙니다.
'''), code('''
    # 이벤트 종류별 대표 한 건을 출력합니다. 중첩 필드를 확인하세요.
    for kind in ("mode_snapshot", "door_open", "door_close"):
        sample = next(e for e in events if e["eventType"] == kind)
        print(kind)
        display(sample)
    summary = Counter(e["eventType"] for e in events)
    display(pd.DataFrame([{"eventType": k, "count": v, "sharePercent": round(v / len(events) * 100, 2)}
                          for k, v in summary.items()]))
    assert summary == {"mode_snapshot": 3360, "door_open": 800, "door_close": 799}
    ids = [e["_id"] for e in events]
    assert len(ids) == len(set(ids))
    master_ids = {d["_id"] for d in devices}
    assert all(e["deviceId"] in master_ids for e in events)
'''), code('''
    # UTC 저장값과 한국 시간 표시를 비교합니다. 일별 집계는 timezone을 명시해야 합니다.
    start, end = min(e["eventTime"] for e in events), max(e["eventTime"] for e in events)
    display(pd.DataFrame([{"boundary": label, "UTC": t.isoformat(), "Asia/Seoul": t.astimezone(KST).isoformat()}
                          for label, t in (("first", start), ("last", end))]))
    delays = Counter((e["receivedAt"] - e["eventTime"]).total_seconds() for e in events)
    print("수신 지연 분포(초):", dict(delays))
    assert delays == {0.3: 4959}
    # 모델별 8대, 총 24대를 기기 마스터에서 계산합니다.
    models = Counter(d["product"]["modelCode"] for d in devices)
    print("모델별 기기 수:", dict(models))
    assert len(models) == 3 and set(models.values()) == {8}
'''), md('''
    ### 데이터 품질과 해석 주의
    모든 원본 지연이 300ms인 것은 **fixture 정의**이며 실제 Kafka/Atlas 지연 측정이 아닙니다.
    원본에 중복/역전 데이터가 없으므로 Lab 3에서 별도 시나리오를 구성합니다.
    `lab2.fullyObserved=true`, `observedDays=7`은 합성 코호트 계약입니다.
    이벤트가 없는 장치도 관측 완료라는 가정으로 0회를 포함할 수 있으며 운영에서는 별도 가용성 근거가 필요합니다.
'''), code('''
    # OPEN/CLOSE 참조, 완료 시간, 미완료 구간을 로컬에서 검증합니다.
    by_id = {e["_id"]: e for e in events}
    opens = {e["_id"] for e in events if e["eventType"] == "door_open"}
    closes = [e for e in events if e["eventType"] == "door_close"]
    referenced = []
    durations = []
    for close in closes:
        open_id = close["payload"]["openEventId"]
        opened = by_id[open_id]
        assert opened["eventType"] == "door_open"
        assert opened["deviceId"] == close["deviceId"] and opened["resource"] == close["resource"]
        assert opened["eventTime"] == close["payload"]["openedAt"] <= close["eventTime"]
        referenced.append(open_id)
        durations.append((close["eventTime"] - opened["eventTime"]).total_seconds())
    assert len(referenced) == len(set(referenced)) == 799
    unfinished = opens - set(referenced)
    assert unfinished == {"LAB2-D01-open-unfinished"}
    print("미완료 OPEN:", unfinished)
    display(pd.Series(durations, name="durationSeconds").describe().to_frame())
    assert sum(durations) == 35997 and min(durations) == 15 and max(durations) == 240
    open_counts = Counter(e["deviceId"] for e in events if e["eventType"] == "door_open")
    cohort = pd.DataFrame([{"deviceId": d["_id"], "modelCode": d["product"]["modelCode"],
        "openCount": open_counts[d["_id"]], "fullyObserved": d["lab2"]["fullyObserved"],
        "observedDays": d["lab2"]["observedDays"]} for d in devices])
    display(cohort)
    assert list(cohort.loc[cohort.openCount == 0, "deviceId"]) == ["LAB2-D23", "LAB2-D24"]
'''), code('''
    # 장표의 대표 정답을 실제 Lab2 fixture에서 계산합니다. 한국 시간 10/11 하루입니다.
    start = datetime(2026, 10, 10, 15, tzinfo=UTC)
    end = start + timedelta(days=1)
    selected_open = [e for e in events if e["deviceId"] == "LAB2-D01" and e["eventType"] == "door_open"
                     and start <= e["eventTime"] < end]
    selected_close = [e for e in closes if e["deviceId"] == "LAB2-D01" and start <= e["eventTime"] < end
                      and start <= e["payload"]["openedAt"] < end]
    seconds = [(e["eventTime"] - e["payload"]["openedAt"]).total_seconds() for e in selected_close]
    assert len(selected_open) == 4 and sorted(seconds) == [20, 40, 240]
    display(pd.DataFrame([{"device": "LAB2-D01", "KST date": "2026-10-11", "opens": len(selected_open),
        "completed": len(seconds), "sumSeconds": sum(seconds), "avgSeconds": sum(seconds) / len(seconds)}]))
    # 미완료 OPEN을 0초로 평균에 넣으면 업무 정의가 달라집니다.
    print("완료 구간 평균 100초. 미완료 OPEN은 0초가 아니라 아직 완료되지 않은 구간입니다.")
'''), md('''
    ## 4. 전체 실습 데이터 흐름

    ```text
    Lab 1: 기기 마스터 + A snapshot 이력 → Entity별 최신 상태·복합키·부분 갱신 이해
                ↓ 개념 이어가기 (Lab 1과 Lab 2 기기 ID는 다름)
    Lab 2: 24대의 mode/door 이력 → 최신 조회·OPEN 횟수·완료 시간·코호트·explain
                ├─ Lab 3: 재전송/역전 이벤트 → 최신 상태 → timer → 감지/해소 결과
                └─ Lab 4: 완료 구간 duration + 합성 온도 → 일반/TS → 집계·비용·TTL
    Lab 5: 모델 코드 → 합성 매뉴얼 chunks → 임베딩 → Search/Vector/Hybrid → 근거·RAG 프롬프트
    ```

    모든 Lab은 전용 DB를 사용하여 집계/테스트 문서를 분리합니다.
    장표 순서대로 교육을 진행하지만 노트북 파일 입력은 독립적입니다.
    참가자 조회 노트북은 해당 Lab의 강사 준비 완료 상태만 필요합니다.
'''), md('''
    ## 5. Lab별 입력·작업·확인 결과

    | Lab / 시간 | 입력 | 🔴 강사 준비·시연 | 🟢 참가자 확인 | 완료 증거 |
    |---|---|---|---|---|
    | [Lab 1](lab1/Lab1_Data_Modeling.ipynb) / 60분 | lab1 EJSON | 적재·키 인덱스·복사본 부분 갱신 | 구조·키·배열·이력 상태 비교 | A 최신 상태 6건 일치 |
    | [Lab 2](lab2/Lab2_Aggregation_and_Indexes.ipynb) / 40분 | lab2 EJSON | 적재·조회 인덱스 생성 | 일별/시간대/완료/모델 집계·explain | 4 OPEN / 3 completed / 300초 / 평균100초 |
    | [Lab 3](lab3/Lab3_Event_Quality_and_Anomaly.ipynb) / 40분 | lab2 + 경계 시나리오 | 재전송·상태 갱신·timer·CLOSE 해소 | 원본 이력·ORDER 상태·알림 조회 | 이력4959 유지 / CLOSED 유지 / 180·181초 감지 |
    | [Lab 4](lab4/Lab4_Time_Series.ipynb) / 30분 | duration799 + 합성온도3456 | 일반/TS 생성·적재·인덱스·선택TTL | 동일 집계·options·그래프·비용 | 일반/TS 일치 / D01 온도144점 |
    | [Lab 5](lab5/Lab5_Search_Vector_RAG.ipynb) / 35분 | 합성매뉴얼12 | 적재·임베딩·검색 인덱스 | 키워드·의미·필터·근거·평가 | 모델 혼입 없음 / 출처 확인 / 검색·답변 평가 구분 |

    **Lab 3 시나리오는 한 강사가 순서대로 수행합니다.** "1회 작업"은 여러 참가자가 동시에 수행하지 않는다는 뜻입니다.
    의도적 재전송/timer 반복은 강사 시연 범위입니다. 원본 `_id` upsert가 멱등해도 변경 작업이므로 강사 전용입니다.
    Time Series insertMany는 재실행 시 중복 가능하며 TTL은 시간 경과로 결과가 달라집니다.
'''), md('''
    ## 6. 강사·참가자 전체 진행 순서

    ### 수업 전 — 강사 1명
    1. 저장소 clone, 가상환경/requirements, `HRM Workshop (Python)` 커널을 준비합니다.
    2. Atlas 접속·DB 쓰기/인덱스 권한·IP 허용 목록을 확인합니다.
    3. 각 Lab의 빨간 셀을 확인하고 `INSTRUCTOR_MODE=True`로 준비합니다. 배포 기본값은 False입니다.
    4. Lab 3 시나리오·Lab 4 측정값·Lab 5 모델 다운로드 및 검색 인덱스 빌드는 미리 준비합니다.
    5. 참가자 계정은 가능하면 read 권한으로 제공하고, 비밀번호는 노트북/출력에 저장하지 않습니다.
    6. `.venv/bin/python validate_notebooks.py`로 참가자 노트북을 점검합니다.
       강사 쓰기 검증은 `... validate_notebooks.py 'lab1/*.ipynb' --instructor`처럼 명시합니다.

    ### 수업 중 — 참가자 전체
    1. 이 가이드로 데이터 구조와 fixture 가정을 확인합니다.
    2. 강사의 Lab별 준비 완료 안내 후 참가자 노트북 해당 섹션을 실행합니다.
    3. 결과 표·assertion·explain을 보고 기대 결과와 다르면 원인/조건을 기록합니다.
    4. 빨간 셀의 변경 시연은 강사 화면으로 확인합니다. 공유 DB 원래 Lab 전체 Run All은 하지 않습니다.
    5. 개인 변경 실습이 필요하면 강사가 정한 참가자별 별도 DB/컬렉션으로 분리한 다음 수행합니다.

    ### 마무리
    - 쓰기 정확성(키·순서·멱등성), 분석 정확성(기간·분모·미완료), 검색 정확성(모델·근거)을 따로 기록합니다.
    - 실습 성공을 운영 성능이나 SLA 달성으로 환산하지 않습니다.
    - 공유 DB 삭제·초기화는 강사가 수업 종료 후 별도 계획으로 수행합니다.
'''), md('''
    ## 7. Kafka Connect 확장 흐름
    ```text
    Producer → Kafka 토픽 ─┬─ MongoDB Sink → 원본 이력
                           └─ 업무 Consumer → 최신 상태·timer·감지 결과
    측정 변환 → 측정 토픽 → Time Series Sink
    문서/chunk 변환 → 문서 토픽 → 일반 컬렉션 → 임베딩 처리/인덱스 동기화 → 검색
    ```
    Broker bootstrap 주소, Connect REST 주소, Atlas URI는 서로 다른 연결입니다.
    Connect의 RUNNING과 Atlas 적재 완료, 검색 인덱스 동기화 완료도 구분합니다.
    Kafka 등록/발행/consumer/offset 재처리는 강사 단독, 상태GET/lag/Atlas 조회는 참가자 동시 가능합니다.
    Lab 3~5 끝에 worker 준비, 설정 JSON, REST 등록, 메시지 날짜 타입, 검증 체크리스트가 있습니다.
    현재 저장된 실행 결과는 PyMongo 직접 적재 결과이며 Kafka end-to-end 실행 결과가 아닙니다.
'''), md('''
    ## 8. 확장·지원 조건과 결과 해석
    - 일반/TS 저장량·검사량은 fixture 관측값입니다. 운영량으로 다시 검증해야 합니다.
    - 모델별 매뉴얼은 각 4건으로 작으므로 Recall@5=1만으로 검색 품질을 주장하지 않습니다.
    - 수동 임베딩: 문서와 질문에 같은 모델·revision·차원을 사용합니다.
    - autoEmbed/native rerank: 배포 버전·Atlas 프로젝트 설정을 확인한 선택 실습입니다.
    - 기본 RAG: 근거 발췌와 프롬프트 작성. 실제 LLM 생성 및 문장별 근거 평가는 별도 연결 단계입니다.
    - 강사가 완료 시점의 결과를 저장했으므로 노트북 출력은 현재 DB 상태와 다를 수 있습니다. 참가자 조회로 재확인하세요.

    ## 결과 기록 템플릿
    | 항목 | 기록 |
    |---|---|
    | Lab / 수행 시각 / 담당 | |
    | 입력 파일·태그 / DB·컬렉션 / 기간·timezone | |
    | 기대 결과 / 실제 결과 / 차이 원인 | |
    | 변경 담당 / 참가자 조회 완료 여부 | |
    | 다음 검증 과제 | |

    [공유 DB 참가자용 노트북](Shared_DB_Participant.ipynb) · [저장소 README](README.md)
''')]
notebook = nbf.v4.new_notebook(cells=cells, metadata={
    "kernelspec": {"display_name": "HRM Workshop (Python)", "language": "python", "name": "hrm-workshop"},
    "language_info": {"name": "python"}})
nbf.validate(notebook)
nbf.write(notebook, ROOT / "Lab_Guide.ipynb")
print("Lab_Guide.ipynb 생성 완료")
