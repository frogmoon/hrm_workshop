"""기존 실행 결과를 보존하며 Kafka Markdown 가이드만 갱신합니다."""

import ast
import json
import re
from pathlib import Path
import nbformat
from kafka_notebook_guide import kafka_guide_cells

root = Path(__file__).resolve().parent
for lab in ("lab3", "lab4", "lab5"):
    path = next((root / lab).glob("*.ipynb"))
    notebook = nbformat.read(path, as_version=4)
    # 기존 안내 셀만 교체하고 원래 코드 셀/출력은 보존합니다.
    original = [c for c in notebook.cells if "kafka-connection-guide" not in c.metadata.get("tags", [])]
    output_count = sum(len(c.get("outputs", [])) for c in original)
    guide = kafka_guide_cells(lab)
    for cell in guide:
        for language, snippet in re.findall(r"```(python|json)\n(.*?)```", cell.source, re.S):
            # 가이드 코드도 Python 구문/JSON 형식 검사를 수행합니다.
            ast.parse(snippet) if language == "python" else json.loads(snippet)
    notebook.cells = original + guide
    nbformat.validate(notebook)
    assert sum(len(c.get("outputs", [])) for c in notebook.cells) == output_count
    nbformat.write(notebook, path)
    print(f"{lab}: Kafka 가이드 2셀 추가 / 기존 출력 {output_count}개 보존")
