"""Lab별 Kafka 연결 가이드 셀을 생성합니다. 네트워크 작업은 실행하지 않습니다."""

import textwrap
import nbformat


COMMON = r'''
## Kafka Connect 연결 가이드 — 준비와 연결 흐름

이 섹션은 Kafka 환경에서 실습을 확장하는 **가이드**입니다. 아래 코드는 Markdown 예시이므로
Run All로 Kafka 등록·발행이 자동 실행되지 않습니다. 실제 환경의 주소·인증을 채운 후 별도 셀/터미널에서 실행하세요.
현재 저장된 실행 결과는 PyMongo 직접 적재 결과입니다.

```text
노트북 Producer ── bootstrap.servers ──▶ Kafka Broker / 토픽
                                           │
                         Kafka Connect Worker (MongoDB Sink plugin)
                                           │ connection.uri
                                           ▼
                                     MongoDB Atlas

노트북 ── Connect REST API (:8083 예시) ──▶ Connector 등록·상태 확인
```

### 1. 주소 세 가지를 구분합니다

| 주소 | 용도 | 교육용 예시 |
|---|---|---|
| Kafka bootstrap server | Producer/Consumer와 Connect worker가 Broker 접속 | `localhost:9092` |
| Connect REST URL | Connector 등록·설정 검증·상태 확인 | `http://localhost:8083` |
| MongoDB URI | Connect worker가 Atlas 접속 | `mongodb+srv://<user>:<password>@<cluster>/` |

Docker 내부 worker의 `localhost`는 worker 컨테이너 자신입니다. Broker의 advertised.listeners는
노트북과 worker 각각에서 접근 가능해야 합니다(예: 호스트 `localhost:9092`, 내부 `kafka:29092`).
Atlas Network Access에는 **Connect worker의 출구 IP**가 필요합니다. 노트북 접속 성공만으로 worker 접속이 보장되지 않습니다.
클라우드 Kafka라면 Broker/REST 각각의 TLS·SASL·인증 정보를 해당 서비스 방식으로 적용하세요.

### 2. Worker 준비

1. Kafka Broker와 Kafka Connect distributed worker를 준비합니다.
2. MongoDB Kafka Connector 배포 JAR/패키지를 **모든 worker의 plugin.path**에 설치하고 worker를 재시작합니다.
3. worker의 `bootstrap.servers`와 내부 config/offset/status 토픽을 설정합니다.
   단일 Broker 실습은 내부 토픽 replication factor를 `1`로 설정합니다. 운영 값은 Broker 구성에 맞춥니다.
4. `GET /connector-plugins`에 `com.mongodb.kafka.connect.MongoSinkConnector`가 보이는지 확인합니다.
5. MongoDB URI는 worker의 ConfigProvider로 주입합니다. OpenCode MCP 설정은 Connect에 자동 전달되지 않습니다.

worker properties 예시(실제 worker 설정 파일에 추가):

```properties
# Kafka 표준 파일 ConfigProvider를 worker에서 활성화합니다.
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

모든 worker에서 읽을 수 있는 `/run/secrets/mongodb.properties`에 `uri=<실제 URI>`를 저장하고
접근 권한을 제한합니다. 아래 Connector JSON은 `${file:/run/secrets/mongodb.properties:uri}`를 참조합니다.
이 경로는 **worker 파일시스템 경로**입니다. 노트북의 로컬 인증 파일 경로와 다릅니다.

### 3. REST 등록 순서

아래 `sink-config.json`은 각 Lab에서 제공하는 **config 객체만** 담은 파일입니다.

```bash
# plugin 설치 상태를 확인합니다.
curl --fail-with-body http://localhost:8083/connector-plugins

# 등록 전에 Connector 설정을 검증합니다. error_count=0인지 확인합니다.
curl --fail-with-body -X PUT \
  http://localhost:8083/connector-plugins/com.mongodb.kafka.connect.MongoSinkConnector/config/validate \
  -H 'Content-Type: application/json' --data-binary @sink-config.json

# 예시 이름 hrm-lab-sink로 Connector 설정을 등록/갱신합니다.
curl --fail-with-body -X PUT http://localhost:8083/connectors/hrm-lab-sink/config \
  -H 'Content-Type: application/json' --data-binary @sink-config.json

# Connector와 모든 task가 RUNNING인지 확인합니다.
curl --fail-with-body http://localhost:8083/connectors/hrm-lab-sink/status
```

`/status`의 task가 FAILED이면 trace와 worker 로그를 확인합니다. 설정 검증 성공은 실제 Atlas 접속 성공이 아닙니다.

### 4. 메시지 계약과 오류 처리

- `lab2/events.ejson`은 **JSON 배열**입니다. 배열 전체를 한 메시지로 보내지 말고 이벤트 객체 한 건씩 발행합니다.
- 발행 재시도에도 이벤트 `_id`를 유지합니다. Kafka key는 기기+리소스 조합으로 설정하여 같은 상태 키를 같은 partition에 보냅니다.
- partition 내부 전달 순서는 eventTime 순서와 다를 수 있습니다. 같은 시각 판정·부분 payload는 생산자 버전 계약이 필요합니다.
- Connect는 재시작/offset 재처리 시 같은 레코드를 재전달할 수 있습니다. 아래 기본 예시를 end-to-end exactly-once로 해석하지 않습니다.
- 기본 예시는 `errors.tolerance=none`으로 실패를 드러냅니다. 오류를 넘기려면 DLQ와 경보를 함께 구성합니다.
  단일 Broker 예시: `errors.tolerance=all`, `errors.deadletterqueue.topic.name=hrm.dlq`,
  `errors.deadletterqueue.topic.replication.factor=1`, `errors.deadletterqueue.context.headers.enable=true`.
  적용 후 정상 건수+DLQ 건수+offset/lag를 함께 확인하고 실패 원인 수정 후 재처리합니다.

### 5. 공통 확인표

| 관측 지점 | 확인 항목 |
|---|---|
| Producer | 발행 성공 callback/flush, 실제 발행 건수 |
| Kafka | topic/partition/offset, consumer group lag |
| Connect | Connector 및 task RUNNING, worker 로그, 실패/DLQ |
| Atlas | 대상 namespace, `_id`, BSON Date 타입, 건수·집계 |

`RUNNING`만으로 완료를 판정하지 말고 특정 이벤트 ID의 Atlas 도착까지 확인합니다.

공식 가이드:
[설치](https://www.mongodb.com/docs/kafka-connector/current/introduction/install/) ·
[전체 설정](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/all-properties/) ·
[ID 전략](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/fundamentals/post-processors/) ·
[쓰기 전략](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/fundamentals/write-strategies/)
'''

LAB3 = r'''
## Lab 3과 Kafka 연결 — 원본 이력과 상태 처리

```text
Lab2 이벤트 → hrm.lab3.events ─┬─ MongoDB Sink → hrm_workshop_lab3.kafka_events
                              └─ 업무 Consumer → current_state → timer → anomaly_results
```

Sink는 원본 이력, Consumer는 최신 상태·조건부 갱신·timer를 담당합니다.
Consumer와 Sink는 서로 독립된 consumer group이므로 어느 쪽이 먼저 처리되는지는 보장되지 않습니다.
이 노트북의 `process_event()`는 이력도 저장하므로 Kafka로 확장할 때 **이력 소유자**를 정하세요.
예시 흐름에서는 Sink만 `kafka_events`를 쓰고, Consumer는 상태 갱신 로직을 별도 함수로 분리합니다.
Consumer 재시작 시 상태 처리 완료 후 offset을 commit하고, 재처리해도 같은 결과가 되도록 유지합니다.

### 일반 이력 Sink 설정 예시

```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "topics": "hrm.lab3.events",
  "connection.uri": "${file:/run/secrets/mongodb.properties:uri}",
  "database": "hrm_workshop_lab3",
  "collection": "kafka_events",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
  "document.id.strategy.overwrite.existing": "false",
  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
  "bulk.write.ordered": "true",
  "errors.tolerance": "none"
}
```

value는 UTF-8 **Extended JSON 문자열**로 발행합니다. StringConverter는 문자열을 전달하고
MongoDB Connector가 JSON 문자열을 BSON으로 읽는 경로를 사용합니다. 원본 `$date`를 일반 중첩 객체로
취급하는 schemaless JsonConverter 예시와 혼용하지 마세요. 설치한 Connector 버전에서 실제 BSON Date를 검증합니다.
Avro/Protobuf/Schema Registry를 사용하는 환경은 해당 converter와 Date/Timestamp 논리 타입을 별도로 맞춥니다.

`ProvidedInValueStrategy`는 value의 `_id`를 사용합니다. ReplaceOne은 동일 ID를 upsert하므로
동일 payload 재발행 시 문서 수가 유지됩니다. **기존 원본을 불변으로 유지하는 `$setOnInsert`와는 다른 의미**이며
같은 ID로 다른 payload를 보내면 내용이 교체됩니다. 최초 값 보존이 필요하면 custom WriteModelStrategy나 consumer를 사용합니다.
`UpdateOneTimestampsStrategy`의 `_modifiedTS`는 처리 시각으로, eventTime 기반 최신 상태 판정을 대신하지 않습니다.

### 노트북 Producer 예시

먼저 선택 의존성 `confluent-kafka`를 설치합니다(`%pip install confluent-kafka`).
이 셀을 별도 실행할 때 Kafka 토픽을 준비하고 실제 bootstrap 주소를 적용합니다.

```python
# 현재 노트북에서 읽은 datetime을 Extended JSON $date로 직렬화합니다.
import json
from bson import json_util
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
delivery_errors = []

def delivered(error, message):
    # 비동기 발행 오류를 누락하지 않고 flush 후 확인합니다.
    if error is not None:
        delivery_errors.append(str(error))

for event in events:
    resource = event["resource"]
    key = json.dumps([event["deviceId"], resource["href"], resource["unitId"], resource["component"]])
    producer.produce("hrm.lab3.events", key=key.encode("utf-8"),
                     value=json_util.dumps(event).encode("utf-8"), on_delivery=delivered)
    producer.poll(0)
assert producer.flush(30) == 0 and not delivery_errors
```

### 단계별 검증

1. 새로운 `kafka_events` 컬렉션/전용 토픽으로 시작합니다. 기존 `events`의 직접 적재 결과와 섞지 않습니다.
2. 4,959건 발행 → Atlas 고유 ID 4,959건인지 확인합니다. lag가 0이 될 때까지 기다립니다.
3. 같은 이벤트 10건 재발행 → 이력 건수 유지, ID와 payload 동일 여부 확인.
4. OPEN→CLOSE→과거 OPEN 발행 → 업무 Consumer가 CLOSED를 유지하는지 확인.
5. OPEN만 발행 → **이벤트 수신과 독립된 timer tick**으로 180초 감지.
6. CLOSE 재처리 → 같은 열림 구간 알림 해소, consumer 재시작 후 중복 알림 없는지 확인.

```python
# Kafka 발행 후 연결 셀을 다시 실행해 종료된 client를 새로 연결합니다.
client, db = connect("hrm_workshop_lab3")
sample = db.kafka_events.find_one({"_id": events[0]["_id"]})
assert sample is not None and isinstance(sample["eventTime"], datetime)
assert db.kafka_events.count_documents({"datasetTag": "HRM_LAB2_DOOR_V1"}) == 4959
client.close()
```

### S3 이력과의 연결

Kafka→Atlas는 실시간 적재 경로입니다. 과거 JSON을 S3에 보관한다면 별도 S3 Sink/아카이빙 작업을 준비하고
Atlas Data Federation에서 Atlas와 S3 데이터 소스를 정의하여 조회할 수 있습니다.
중복 기간·eventId·날짜 타입·스키마를 맞춘 뒤 합산합니다. 이 Sink 설정만으로 S3 보관이 생성되지는 않습니다.
'''

LAB4 = r'''
## Lab 4와 Kafka 연결 — 측정 토픽에서 Time Series로

```text
원본 이벤트 → 일반 이력 (중복 검증)
                   ↓ 측정 변환 / 수집 checkpoint
          hrm.lab4.temperature → MongoDB Sink → kafka_temperature_ts
```

Lab2 원본에는 온도가 없습니다. 이 노트북의 `temperature_measurements`는 합성 온도이며
`door_measurements`는 원본 OPEN/CLOSE에서 계산한 완료 구간입니다.
Connector는 문 열림 시간을 자동 계산하지 않습니다. Producer/업무 처리기가 먼저 측정 문서로 변환합니다.

### Time Series Sink 설정 예시

```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "topics": "hrm.lab4.temperature",
  "connection.uri": "${file:/run/secrets/mongodb.properties:uri}",
  "database": "hrm_workshop_lab4",
  "collection": "kafka_temperature_ts",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "timeseries.timefield": "eventTime",
  "timeseries.timefield.auto.convert": "true",
  "timeseries.metafield": "meta",
  "timeseries.granularity": "minutes",
  "timeseries.expire.after.seconds": "0",
  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.InsertOneDefaultStrategy",
  "errors.tolerance": "none"
}
```

`eventTime`をepoch millisecondsの数値として送り、Connectorのauto.convertでBSON Dateへ変換します。
文字列を使う場合はJava DateTimeFormatter形式も合わせます。変換失敗は元の値が渡るので
Atlasが求めるDateにならず書き込み失敗する可能性があります。
TTL=0은 고정된 2026년 데이터가 사라지지 않게 하는 실습 설정입니다.
기존 컬렉션의 TTL이 자동 해제된다고 가정하지 말고 options를 확인합니다.

새 `kafka_temperature_ts`를 사용하고, 미리 생성한다면
`timeField=eventTime, metaField=meta, granularity=minutes`를 일치시킵니다.
일반 컬렉션이 같은 이름으로 존재하면 다른 이름을 사용합니다.

```python
# 앞에서 만든 합성 측정값을 한 문서씩 JSON으로 변환하여 발행합니다.
import json
from copy import deepcopy
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
delivery_errors = []

def delivered(error, message):
    # 발행 성공/실패를 flush 후 확인합니다.
    if error is not None:
        delivery_errors.append(str(error))

for measurement in temperature_measurements:
    wire = deepcopy(measurement)
    wire["eventTime"] = int(measurement["eventTime"].timestamp() * 1000)
    producer.produce("hrm.lab4.temperature", key=wire["meta"]["deviceId"].encode("utf-8"),
                     value=json.dumps(wire).encode("utf-8"), on_delivery=delivered)
    producer.poll(0)
assert producer.flush(30) == 0 and not delivery_errors
```

### 검증과 중복 처리의 한계

1. 신규 토픽·빈 컬렉션에 3,456건을 **한 번** 발행합니다.
2. 컬렉션 options의 timeseries 설정, eventTime BSON Date, 건수 3,456을 확인합니다.
3. 본 노트북의 동일 `pipeline`으로 직접 적재 온도 집계와 Kafka 적재 집계를 비교합니다.
4. 재발행/offset 재처리는 **측정 중복을 만들 수 있습니다**. Time Series의 `_id`는 unique 중복 제거가 아닙니다.
5. 일반 이력에서 중복 판정해도 일반 이력/Time Series/offset 사이의 원자성이 자동 보장되지 않습니다.
   재시작 checkpoint·재조정(reconciliation)·집계 시 sourceEventId 중복 판정 정책을 설계합니다.

```python
# 직접 적재와 Kafka 적재의 동일 조건 집계를 비교합니다. 중복 발행이 없다는 조건입니다.
client, db = connect("hrm_workshop_lab4")
assert db.kafka_temperature_ts.options()["timeseries"]["timeField"] == "eventTime"
assert db.kafka_temperature_ts.count_documents({}) == 3456
assert list(db.kafka_temperature_ts.aggregate(pipeline)) == regular_result
client.close()
```

문 열림 시간을 스트리밍하려면 별도 `hrm.lab4.door_duration` 토픽에서 `durationSeconds`를 보내고
다른 Sink/컬렉션을 사용합니다. `meta`에는 기기·고정 리소스·모델만 넣습니다.
[Kafka Time Series 설정](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/time-series/)
'''

LAB5 = r'''
## Lab 5와 Kafka 연결 — 문서 적재 이후 검색까지

```text
문서 변경 → 전처리/chunk 생성 → hrm.lab5.manual.chunks
                                      ↓ MongoDB Sink (일반 컬렉션)
                                 manual_chunks
                                      ↓ 임베딩 처리기 / 인덱스 동기화
                             Atlas Search + Vector Search
                                      ↓ 근거 검색
                                 RAG 프롬프트 → LLM
```

- Connector는 본문을 Atlas에 전달합니다. chunk 분할·외부 모델 임베딩·RAG 답변 생성은 별도 처리입니다.
- 기본 노트북의 수동 임베딩 방식에서는 **chunk → embedding 생성 → 해당 embedding 필드 갱신** 작업이 필요합니다.
- 검색 인덱스는 비동기로 동기화됩니다. Connector RUNNING/문서 적재와 검색 가능 시점은 다릅니다.
- autoEmbed 지원 환경에서는 텍스트를 적재하면 해당 인덱스가 임베딩을 관리합니다. 현재 수동 embedding 필드와 구분합니다.

### 일반 문서 Sink 연결 기준

Lab3의 일반 이력 JSON을 기준으로 `topics=hrm.lab5.manual.chunks`,
`database=hrm_workshop_lab5`, `collection=manual_chunks`로 바꾸고
`ProvidedInValueStrategy`로 안정적인 chunk ID를 사용합니다.
기존 문서의 `embedding`을 별도 처리기가 생성한다면 **ReplaceOne으로 본문만 재적재할 때 embedding이 제거**됩니다.
이 경우 `UpdateOneDefaultStrategy`로 전달 필드만 갱신하는 방식을 검토하고,
본문 변경 시 기존 임베딩을 유효하다고 취급하지 않도록 `chunkVersion`/본문 해시 기반 재임베딩 상태를 관리합니다.

문서 전체를 완전 교체하는 계약이면 ReplaceOne을 사용하고 동일 버전의 embedding까지 함께 보내세요.
필드 삭제가 필요한 경우 UpdateOne의 부분 갱신만으로 누락 필드가 제거되지는 않으므로 별도 삭제 계약이 필요합니다.

### 검색 도착 검증

1. chunk `_id`, 모델·국가·언어, source/page, chunkVersion을 정합니다.
2. Kafka 발행 완료 → Atlas 해당 chunk 도착을 확인합니다.
3. 수동 모델 임베딩의 모델/revision/차원과 본문 버전이 일치하는지 확인합니다.
4. Search/Vector 인덱스 queryable을 확인하고 **해당 chunk를 찾는 질의**로 동기화까지 확인합니다.
5. 노트북의 정답 질문·모델 필터·Recall/MRR을 다시 평가합니다.
6. RAG 근거의 source/page가 적재한 버전의 원문을 가리키는지 확인합니다.

Kafka Connect의 offset commit은 검색 인덱스 동기화나 LLM 답변 완료를 기다리는 신호가 아닙니다.
S3/PDF 원문은 source로 참조하고, PDF binary를 Search 텍스트처럼 직접 넣지 않습니다.
'''


def kafka_guide_cells(lab):
    """가이드의 Python/JSON/bash는 설명용 fenced block으로 추가합니다."""
    specific = {"lab3": LAB3, "lab4": LAB4, "lab5": LAB5}[lab]
    cells = []
    for index, text in enumerate((COMMON, specific)):
        cell = nbformat.v4.new_markdown_cell(textwrap.dedent(text).strip())
        cell.id = f"kafka-guide-{lab}-{index}"
        cell.metadata["tags"] = ["kafka-connection-guide"]
        cells.append(cell)
    return cells
