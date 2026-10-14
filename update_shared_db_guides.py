"""이미 실행한 Lab 결과를 유지하면서 공유 DB 역할 안내를 추가합니다."""

from pathlib import Path
import nbformat
from shared_db_guidance import annotate_shared_db

for path in sorted(Path(__file__).resolve().parent.glob("lab[12345]/*.ipynb")):
    notebook = nbformat.read(path, as_version=4)
    previous_outputs = sum(len(c.get("outputs", [])) for c in notebook.cells)
    annotate_shared_db(notebook, path.parent.name)
    nbformat.validate(notebook)
    assert sum(len(c.get("outputs", [])) for c in notebook.cells) == previous_outputs
    nbformat.write(notebook, path)
    print(path.parent.name, "역할 표시 및 강사 쓰기 차단 / 기존 결과 보존")
