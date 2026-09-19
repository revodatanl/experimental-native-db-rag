"""Start the agent server and the chat UI in one container.

Databricks Apps routes traffic to port 8000, where the agent server listens. The UI,
Databricks' e2e-chatbot-app-next template, is cloned on the first start and runs on
port 3000. The agent server proxies the UI's pages; the UI proxies /invocations back
to the agent. Both processes write to stdout, so their logs are the app's logs.
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

UI_DIR = Path("e2e-chatbot-app-next")
UI_REPO = "https://github.com/databricks/app-templates.git"
AGENT_PORT = 8000
UI_PORT = 3000


def fetch_ui() -> None:
    """Sparse-clone the UI template once; later starts reuse the checkout."""
    if UI_DIR.exists():
        return
    clone = Path("app-templates")
    subprocess.run(["git", "clone", "--filter=blob:none", "--sparse", UI_REPO, str(clone)], check=True)
    subprocess.run(["git", "sparse-checkout", "set", UI_DIR.name], cwd=clone, check=True)
    (clone / UI_DIR.name).rename(UI_DIR)
    shutil.rmtree(clone, ignore_errors=True)


def main() -> None:
    """Entry point for `uv run start-app`. Exits when either process exits."""
    fetch_ui()
    subprocess.run(["npm", "install"], cwd=UI_DIR, check=True)
    subprocess.run(["npm", "run", "build"], cwd=UI_DIR, check=True)

    agent = subprocess.Popen(["uv", "run", "start-server", "--port", str(AGENT_PORT)])
    # CHAT_APP_PORT is set explicitly because the UI otherwise falls back to PORT,
    # which Databricks Apps sets to the agent's port.
    ui_env = {**os.environ, "API_PROXY": f"http://localhost:{AGENT_PORT}/invocations", "CHAT_APP_PORT": str(UI_PORT)}
    ui = subprocess.Popen(["npm", "run", "start"], cwd=UI_DIR, env=ui_env)

    processes = [agent, ui]
    try:
        while all(process.poll() is None for process in processes):
            time.sleep(1)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait(timeout=10)
    sys.exit(1 if any(process.returncode for process in processes) else 0)
