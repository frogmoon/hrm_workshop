# hrm_workshop

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
- mongodb+srv://main_user:main_user_password@dev.fla1e.mongodb.net/

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

  
