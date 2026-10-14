"""프로젝트 가상환경으로 Jupyter를 띄우고 다섯 실습 노트북을 브라우저에서 엽니다."""

import json
import secrets
import socket
import subprocess
import time
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent

# 다른 Jupyter를 건드리지 않고 사용 가능한 로컬 포트를 선택합니다.
for port in range(8890, 8900):
    with socket.socket() as probe:
        if probe.connect_ex(("127.0.0.1", port)) != 0:
            break
else:
    raise RuntimeError("8890~8899 포트가 모두 사용 중입니다.")

# 인증을 유지하고 loopback 인터페이스에서만 서버를 엽니다.
token = secrets.token_urlsafe(32)
log_path = ROOT / "jupyter.log"
with log_path.open("a") as log:
    process = subprocess.Popen([
        str(ROOT / ".venv/bin/jupyter"), "lab", "--no-browser", "--ip=127.0.0.1",
        f"--port={port}", "--port-retries=0", f"--ServerApp.root_dir={ROOT}",
        f"--IdentityProvider.token={token}"
    ], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

base = f"http://127.0.0.1:{port}"
headers = {"Authorization": "token " + token}

# 기동 완료까지 기다린 뒤 Contents API로 실제 노트북 제공 여부를 검증합니다.
for attempt in range(60):
    if process.poll() is not None:
        raise RuntimeError(f"Jupyter 기동 실패: {log_path}")
    try:
        with urlopen(Request(base + "/api/status", headers=headers), timeout=2) as response:
            if response.status == 200:
                break
    except OSError:
        time.sleep(1)
else:
    raise TimeoutError(f"서버 기동 시간 초과: {log_path}")

for relative in (
    "lab1/Lab1_Data_Modeling.ipynb",
    "lab2/Lab2_Aggregation_and_Indexes.ipynb",
    "lab3/Lab3_Event_Quality_and_Anomaly.ipynb",
    "lab4/Lab4_Time_Series.ipynb",
    "lab5/Lab5_Search_Vector_RAG.ipynb"
):
    with urlopen(Request(base + "/api/contents/" + relative, headers=headers), timeout=5) as response:
        document = json.load(response)
        assert document["type"] == "notebook"
    # 초기 인증 토큰은 브라우저에만 전달하고 결과 출력에는 포함하지 않습니다.
    subprocess.run(["open", base + "/lab/tree/" + relative + "?token=" + token], check=True)
    print("열림:", relative)

print("Jupyter URL:", base + "/lab")
print("Jupyter PID:", process.pid)
