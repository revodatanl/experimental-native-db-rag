"""Start the chat UI and the agent server in one container.

Databricks Apps routes traffic to port 8000, where the agent server listens. The UI,
Databricks' e2e-chatbot-app-next template, is cloned on the first start and runs on
port 3000. The agent server proxies the UI's pages; the UI proxies /invocations back
to the agent. Both write to stdout, so their logs are the app's logs.
"""

import os
import signal
import subprocess
from pathlib import Path

from confluence_rag.agent import server

UI_DIR = Path("app-templates/e2e-chatbot-app-next")
UI_REPO = "https://github.com/databricks/app-templates.git"
# Without CHAT_APP_PORT the UI would take PORT, which Databricks Apps sets to 8000.
UI_ENV = {"CHAT_APP_PORT": "3000", "API_PROXY": "http://localhost:8000/invocations"}


def setup_ui() -> None:
    """Sparse-clone and build the UI template once; later starts reuse the build."""
    if (UI_DIR / "server/dist").exists():
        return
    subprocess.run(["git", "clone", "--filter=blob:none", "--sparse", UI_REPO, str(UI_DIR.parent)], check=True)
    subprocess.run(["git", "sparse-checkout", "set", UI_DIR.name], cwd=UI_DIR.parent, check=True)
    subprocess.run(["npm", "install"], cwd=UI_DIR, check=True)
    subprocess.run(["npm", "run", "build"], cwd=UI_DIR, check=True)


def start_ui() -> None:
    """Run the UI in the background; when Databricks Apps stops this process, stop all of the UI's too."""
    ui = subprocess.Popen(["npm", "run", "start"], cwd=UI_DIR, env=os.environ | UI_ENV, start_new_session=True)
    signal.signal(signal.SIGTERM, lambda *_: os.killpg(ui.pid, signal.SIGTERM))


def main() -> None:
    """Entry point for `uv run start-app`: start the UI, then serve the agent."""
    setup_ui()
    start_ui()
    server.run(app_import_string="confluence_rag.agent:server.app")
