"""공유 DB 실습에서 강사 쓰기와 참가자 읽기를 명시적으로 분리합니다."""

import nbformat

# 각 Lab의 원래 코드 셀 순서 기준입니다. mixed 셀은 쓰기 하나라도 있으면 강사 전용입니다.
WRITE_CELLS = {"lab1": {0, 2, 3, 4}, "lab2": {0, 5}, "lab3": {1, 2, 3, 4, 5},
               "lab4": {3, 6}, "lab5": {1, 2, 3, 8}}
GUARD = '''# [강사 단독 실행] 공유 DB 변경은 지정된 강사 한 명만 수행합니다.
if not INSTRUCTOR_MODE:
    raise RuntimeError("강사 전용 셀입니다. 참가자는 Shared_DB_Participant.ipynb를 실행하세요.")
'''


def annotate_shared_db(notebook, lab):
    """기존 출력은 보존하고 역할 헤더·쓰기 차단만 추가합니다. 반복 호출도 안전합니다."""
    cells = [c for c in notebook.cells if not c.metadata.get("shared_db_generated")]
    index = 0
    result = []
    for cell in cells:
        if cell.cell_type == "code":
            # 이전에 붙인 guard를 제거하고 현재 정책을 다시 적용합니다.
            if cell.source.startswith(GUARD):
                cell.source = cell.source[len(GUARD):]
            writes = index in WRITE_CELLS[lab]
            label = "🔴 강사 단독 실행 · DB 변경" if writes else "🟢 다중 실행 가능 · 조회/로컬 계산"
            description = ("강사 준비·시연 단계에서만 실행합니다. 참가자 동시 실행 및 공유 DB Run All 금지."
                           if writes else "DB를 변경하지 않습니다. 이 노트북의 앞 셀 변수가 필요하면 참가자 전용 노트북을 사용하세요.")
            header = nbformat.v4.new_markdown_cell(f"### {label}\n{description}")
            header.metadata["shared_db_generated"] = True
            result.append(header)
            if writes:
                cell.source = GUARD + cell.source
            cell.metadata["execution_role"] = "instructor_only" if writes else "parallel_read_or_local"
            index += 1
        result.append(cell)
    intro = nbformat.v4.new_markdown_cell('''## 공유 DB 실행 역할 — 먼저 확인하세요

| 표시 | 실행 담당 | 작업 |
|---|---|---|
| 🔴 강사 단독 실행 | 지정 강사 1명 | 적재·인덱스/컬렉션 생성·상태/알림 변경·임베딩 갱신 |
| 🟢 다중 실행 가능 | 모든 참가자 | find/aggregate/explain·건수/타입 확인·로컬 계산 |

**참가자:** 저장소 루트의 `Shared_DB_Participant.ipynb`에서 해당 Lab을 실행하세요.
이 파일의 강사 셀을 생략하면 변수 정의가 빠질 수 있으므로 참가자용은 독립된 읽기 코드로 구성했습니다.
**강사:** 아래 `INSTRUCTOR_MODE = True`로 변경한 뒤 준비/시연하세요. 참가자는 False를 유지합니다.
기본값 False에서는 쓰기 셀이 DB 변경 전에 중단됩니다. 표시된 이전 출력은 강사의 실행 예시입니다.

멱등 upsert도 DB 쓰기이므로 강사 전용입니다. "1회"는 **여러 사람이 실행하지 않는다는 의미**입니다.
재전송·부분 갱신·timer 테스트는 강사 한 명이 시나리오 순서대로 반복합니다.
Time Series insertMany·TTL insert·Kafka 재발행은 반복 시 중복/만료 변화가 발생할 수 있습니다.
공유 데이터 준비가 끝나면 강사가 완료를 안내하고 참가자 조회를 시작합니다.
''')
    intro.metadata["shared_db_generated"] = True
    role = nbformat.v4.new_code_cell('''# 참가자는 False 유지. 지정 강사 한 명만 True로 변경합니다.
INSTRUCTOR_MODE = False''')
    role.metadata["shared_db_generated"] = True
    notebook.cells = [result[0], intro, role, *result[1:]]
    # Kafka 가이드는 조회/변경 명령이 섞여 있으므로 명령 실행 담당을 명시합니다.
    for cell in notebook.cells:
        if "kafka-connection-guide" in cell.metadata.get("tags", []):
            banner = "**공유 환경 역할:** Connector 등록/갱신·Producer 발행·consumer 상태 처리·offset 재처리는 강사 단독 실행입니다. 상태 GET·lag·Atlas 조회는 참가자 동시 실행 가능합니다.\n\n"
            if not cell.source.startswith(banner):
                cell.source = banner + cell.source
    return notebook
