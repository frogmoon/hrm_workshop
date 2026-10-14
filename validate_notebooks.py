"""실제 커널로 노트북을 실행하고 결과·오류를 검증합니다."""

import ast
import sys
from pathlib import Path
import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent
arguments = [a for a in sys.argv[1:] if a != "--instructor"]
for path in sorted(ROOT.glob(arguments[0] if arguments else "Shared_DB_Participant.ipynb")):
    notebook = nbformat.read(path, as_version=4)
    if any(c.metadata.get("execution_role") == "instructor_only" for c in notebook.cells):
        if "--instructor" not in sys.argv:
            raise RuntimeError("강사 DB 변경 검증에는 --instructor를 명시하세요.")
        # 검증 실행의 메모리 안에서만 역할을 활성화하고 저장본은 False로 유지합니다.
        for cell in notebook.cells:
            if cell.cell_type == "code" and cell.metadata.get("shared_db_generated"):
                cell.source = cell.source.replace("INSTRUCTOR_MODE = False", "INSTRUCTOR_MODE = True")
    nbformat.validate(notebook)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            ast.parse(cell.source)
            assert "#" in cell.source, f"주석 없는 셀: {path.name}"
    # 각 노트북을 독립 커널에서 실행하며 오류를 무시하지 않습니다.
    print("실행 시작:", path.name, flush=True)
    NotebookClient(notebook, timeout=900, kernel_name="hrm-workshop",
                   resources={"metadata": {"path": str(path.parent)}}).execute()
    for cell in notebook.cells:
        if cell.cell_type == "code" and cell.metadata.get("shared_db_generated"):
            cell.source = cell.source.replace("INSTRUCTOR_MODE = True", "INSTRUCTOR_MODE = False")
    nbformat.write(notebook, path)
    print("실행 완료:", path.name, flush=True)
    for cell in notebook.cells:
        for output in cell.get("outputs", []):
            if output.output_type == "stream":
                print(output.text.strip(), flush=True)
