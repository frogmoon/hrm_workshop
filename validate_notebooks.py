"""실제 커널로 노트북을 실행하고 결과·오류를 검증합니다."""

import ast
import sys
from pathlib import Path
import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent
for path in sorted(ROOT.glob(sys.argv[1] if len(sys.argv) > 1 else "lab[345]/*.ipynb")):
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            ast.parse(cell.source)
            assert "#" in cell.source, f"주석 없는 셀: {path.name}"
    # 각 노트북을 독립 커널에서 실행하며 오류를 무시하지 않습니다.
    print("실행 시작:", path.name, flush=True)
    NotebookClient(notebook, timeout=900, kernel_name="hrm-workshop",
                   resources={"metadata": {"path": str(path.parent)}}).execute()
    nbformat.write(notebook, path)
    print("실행 완료:", path.name, flush=True)
    for cell in notebook.cells:
        for output in cell.get("outputs", []):
            if output.output_type == "stream":
                print(output.text.strip(), flush=True)
