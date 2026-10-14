# hrm_workshop

## Lab 1·2·3·4·5 Jupyter 실습

**먼저 [Lab Guide — 샘플 분석·전체 워크플로우](Lab_Guide.ipynb)를 확인하세요.**
Lab 1/2 데이터 구조·참조·기간·코호트·기대값을 로컬 파일로 분석하고,
전체 Lab의 입력/작업/확인 결과 및 강사·참가자 순서를 정리했습니다. DB 연결이나 쓰기는 수행하지 않습니다.
`build_lab_guide.py`로 재생성할 수 있습니다.

### 여러 사람이 하나의 DB를 사용할 때

- **참가자:** [공유 DB 참가자 조회 노트북](Shared_DB_Participant.ipynb)을 실행합니다.
  Lab 1~5별 독립 코드로 조회·검증만 수행하며 DB를 변경하지 않습니다.
- **강사 1명:** 각 Lab의 🔴 표시 셀에서 적재·컬렉션/인덱스 생성·갱신·시나리오를 준비합니다.
  `INSTRUCTOR_MODE=True`를 명시해야 실행됩니다. 배포 기본값은 False입니다.
- **🟢 셀:** 조회·확인·로컬 계산은 동시 실행 가능합니다. 기존 Lab의 셀 변수 의존성을 피하려면 참가자 노트북을 사용하세요.
- 데이터 준비 후 강사가 완료를 안내합니다. 참가자는 원래 Lab 전체에 Run All을 실행하지 않습니다.
- 재전송·timer 테스트도 DB 변경이므로 강사만 시연합니다. Time Series·TTL·Kafka 재발행은 중복/상태 변화를 만들 수 있습니다.
- 준비와 조회를 서버 권한으로도 분리하려면 참가자에게 read 역할의 DB 계정을 사용합니다.

| 노트북 | 주요 내용 |
|---|---|
| [Lab 1](lab1/Lab1_Data_Modeling.ipynb) | BSON 타입, Entity별 복합키, 배열 조회, 최신 상태 검증, 부분 갱신·누락/null/빈 배열 |
| [Lab 2](lab2/Lab2_Aggregation_and_Indexes.ipynb) | 일별/시간대별 OPEN, 완료 시간, 모델별 코호트, COLLSCAN/인덱스 explain 비교 |
| [Lab 3](lab3/Lab3_Event_Quality_and_Anomaly.ipynb) | 멱등 적재, 최신 상태, 순서 역전, 180초 timer, 알림 해소 |
| [Lab 4](lab4/Lab4_Time_Series.ipynb) | 문 열림 시간, 합성 온도, 일반/Time Series 집계 비교, explain, TTL |
| [Lab 5](lab5/Lab5_Search_Vector_RAG.ipynb) | 한국어 Search, 실제 다국어 임베딩, Vector Search, Hybrid, RAG 프롬프트·인용·평가 |

### 실행

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m ipykernel install --sys-prefix --name hrm-workshop --display-name "HRM Workshop (Python)"
.venv/bin/jupyter lab
```

각 노트북에서 `HRM Workshop (Python)` 커널을 선택합니다. 공유 환경 참가자는 참가자 조회 노트북을 사용합니다.
MongoDB URI는 `MDB_MCP_CONNECTION_STRING` 환경변수, 로컬 인증 파일
`~/.config/opencode/mongodb-workshop-uri`, 또는 노트북의 숨김 입력으로 제공합니다.
비밀번호는 노트북이나 Git에 저장하지 않습니다.

실습은 `hrm_workshop_lab1`부터 `hrm_workshop_lab5`까지 Lab별 전용 DB에 데이터를 쓰고
일반/Search/Vector Search 인덱스를 생성하므로 DB 쓰기·인덱스 생성 권한이 필요합니다.
검색 인덱스의 비동기 빌드를 기다리며 첫 임베딩 모델 다운로드에 시간이 걸릴 수 있습니다.

### 데이터와 기대 결과

- 원본 `lab2`: 기기 24대, 이벤트 4,959건(mode 3,360 / open 800 / close 799).
- Lab 1: 기존 기기 3건·A 이벤트 18건·상태 12건. 최신 A 상태 6건을 이력과 비교합니다.
- Lab 2: LAB2-D01의 한국 시간 2026-10-11 하루는 OPEN 4회, 완료 3구간, 합계 300초·평균 100초.
  모델별 집계는 관측 완료 기기 24대를 분모로 포함합니다.
- Lab 3: 원본 전체 이력 적재, 상태 갱신은 12개 원본 도어 이벤트 및 별도 합성 경계 시나리오.
  동일 ID 재전송 후 건수 유지, 오래된 OPEN 뒤 CLOSED 유지, 179초 제외·180/181초 감지.
- Lab 4: 실제 CLOSE의 참조 OPEN을 검증한 열림 시간 799건과 **합성 온도 3,456건**.
  일반/Time Series 결과가 일치해야 합니다. 온도는 원본 관측값이 아닙니다.
- Lab 5: **합성 매뉴얼 12건**. 실제 제품 매뉴얼이나 점검 지침이 아닙니다.
  기본 RAG 출력은 검색 발췌 및 LLM 입력 프롬프트이며 LLM 생성 답변은 아닙니다.
- `autoEmbed`, `$rerank`, TTL은 별도 선택 실행 셀입니다. 지원 버전·Atlas 설정을 확인하세요.

`build_notebooks.py`는 노트북 재생성용이며 실행 결과를 초기화합니다.
`build_lab12_notebooks.py`는 Lab 1·2만 재생성합니다.
`validate_notebooks.py`는 노트북을 실행하고 결과를 저장합니다. 강사 Lab 검증에는 실제 DB 쓰기가 포함됩니다.
기본 실행은 참가자 노트북 검증이며, 강사 Lab 실행은 예를 들어
`.venv/bin/python validate_notebooks.py 'lab1/*.ipynb' --instructor`로 명시합니다.
`update_shared_db_guides.py`는 기존 출력 보존 상태에서 역할 표시를 갱신합니다.

### Kafka 연결 가이드

각 노트북 끝의 **Kafka Connect 연결 가이드**에서 Broker/Connect REST/Atlas 연결 흐름,
worker plugin·인증 준비, Sink 설정 JSON, REST 등록, 발행·건수·날짜·lag 검증을 확인할 수 있습니다.
Lab 3은 이력과 상태 consumer의 역할, Lab 4는 Time Series와 중복 처리,
Lab 5는 문서 적재→임베딩→검색 동기화→RAG 흐름을 설명합니다.
예시 코드는 Markdown 가이드이며 실제 Kafka 환경의 주소를 적용하여 선택 실행합니다.
Producer 예시에는 선택 패키지 `confluent-kafka`가 필요합니다.
`update_kafka_guides.py`는 기존 노트북 실행 결과를 보존하면서 가이드만 갱신합니다.

### 세션 일정
- 환경 및 실습 안내 : Atlas 생성, Compass 설치, 데이터셋 및 실습 절차 확인 10:00 - 10:30 (30분)
- Relational Migrator : Relational DB > Atlas로 data migration 10:30 - 11:00 (30분)
- Lab 1: HRM 데이터 모델링 11:00 - 12:00 (60분)
- 점심 12:00 - 13:00 (60분)
- Lab 2: Aggregation과 인덱스 13:00 - 13:40 (40분)
- Lab 3: 이벤트 품질 및 이상 감지 13:40 - 14:20 (40분)
- 휴식 14:20 - 14:35 (15분)
- Lab 4: Time Series Discovery 14:35 - 15:05 (30분)
- Lab 5: Vector Search/RAG Discovery 15:05 - 15:40 (35분)
- 결과 리뷰 및 PoC 일정 논의 15:40 - 16:00 (20분)



### [Session-01] 환경 및 실습 안내 - Atlas 생성, Compass 설치, 데이터셋 및 실습 절차 확인
- Compass 설치: https://www.mongodb.com/docs/compass/
- Connection string
  - mongodb+srv://<db_username>:<db_password>@hands-on-workshop.dmiwn0n.mongodb.net/ 

### [Session-02] Relational Migrator - Relational Migrator를 이용한 데이터 마이그레이션
- Relational Migrator 설치: https://www.mongodb.com/docs/relational-migrator/getting-started/
- MySQL 연결해야 하는 경우, 드라이버 파일을 다운로드하여 Relational Migrator 디렉토리에 복사.
  - MySQL: latest 9.1.x version, Platform Independent Version (https://dev.mysql.com/downloads/connector/j/)
  - 드라이버 파일을 해당 디렉토리에 복사 c:\Users\<username>\AppData\Local\MongoDB\Relational Migrator\Data\Drivers.

- 실습용 PostgreSQL 정보
  - JDBC URI: jdbc:postgresql://migrator-postgres-demo.cjpb4r5dkb0r.us-east-1.rds.amazonaws.com/postgres
  - username: demo
  - password: migrator

### [Session-03] Lab1 HRM 데이터 모델링
- 실습용 데이터 Import
  - Compass에서 파일 가져오기
    - lab1 디렉토리에서 ejson 파일 다운로드
    - compass에서 database > collection에서 Add data -> Import JSON or CSV file 클릭 후 다운로드한 파일 선택

### [Session-04] Lab2 Aggregation과 인덱스
- 실습용 데이터 Import
  - Compass에서 파일 가져오기
    - lab2 디렉토리에서 ejson 파일 다운로드
    - compass에서 database > collection에서 Add data -> Import JSON or CSV file 클릭 후 다운로드한 파일 선택

  
