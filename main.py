# main.py  ── LangGraph 0.3.31 transparent runner
import os
import json
from pathlib import Path
from pprint import pprint
import pandas as pd
import networkx as nx  # Graph export
from state_schema import AgentState
from langgraph_implementation import create_agent_workflow
import traceback
import sys
from pprint import pformat  
import logging
import pathlib
from dotenv import load_dotenv
from typing import Dict

# ── 1️⃣  make sure the log directory exists
log_dir = pathlib.Path("logs")
log_dir.mkdir(exist_ok=True)

# ── 2️⃣  configure root logger: console + file
log_format = "%(asctime)s  %(levelname)-8s %(message)s"
date_fmt   = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(                     # this sets the console handler
    level=logging.INFO,
    format=log_format,
    datefmt=date_fmt,
)

# extra file handler
file_handler = logging.FileHandler(log_dir / "app.log", encoding="utf-8", mode='w')
file_handler.setFormatter(logging.Formatter(log_format, date_fmt))
file_handler.setLevel(logging.INFO)      # or DEBUG if you prefer
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger("DataProcessingApp")

logger.info("🚀  Logging initialised – console + logs/app.log")
print("🚀  Logging initialised – console + logs/app.log")

# ──────────────────────────────────────────────────────────────────────────
# Configuration helpers
# ──────────────────────────────────────────────────────────────────────────
def load_api_keys() -> tuple[str, str]:
    print("\n🔑 API Key Configuration")
    print("────────────────────────")
    
    # Use a try-except block to handle potential EOF errors in WebSocket mode
    try:
        deepseek = input("Please enter your Deepseek API key: ").strip()
        openai = input("Please enter your OpenAI API key: ").strip()
        
        if not (deepseek and openai):
            raise RuntimeError("Both API keys are required to continue")
        
        print("✅ API keys configured successfully\n")
        return deepseek, openai
        
    except EOFError:
        # If we're in WebSocket mode and can't get input, use default keys
        print("⚠️  Running in WebSocket mode - using default API keys")
        deepseek = 'sk-74c415edef3f4a16b1ef8deb3839cf2a'
        openai = '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'
        return deepseek, openai

def load_dataframes() -> dict[str, pd.DataFrame]:
    # Use absolute path to find data files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base = Path(current_dir) / "01_Data"
    
    logger.info(f"Loading data files from {base}")
    
    files = {
        "EFR":  base / "EFR.csv",
        "EQR":  base / "EQR.csv",
        "SKMS": base / "SKMS.csv",
    }
    
    # Check if files exist
    for name, path in files.items():
        if not path.exists():
            logger.error(f"Data file not found: {path}")
            raise FileNotFoundError(f"Data file not found: {path}")
        else:
            logger.info(f"Found data file: {path}")
    
    return {name: pd.read_csv(path) for name, path in files.items()}

def to_dict_if_pairs(x):
    """
    If x looks like [('k', v), …] convert to {k: v}; otherwise return x unchanged.
    """
    if (
        isinstance(x, list)
        and all(isinstance(t, (list, tuple)) for t in x)
    ):
        return dict(x)
    return x

# ──────────────────────────────────────────────────────────────────────────
# Pretty‑printing utilities
# ──────────────────────────────────────────────────────────────────────────
def print_banner(text: str) -> None:
    bar = "─" * len(text)
    print(f"\n\033[1m{bar}\n{text}\n{bar}\033[0m")  # bold ANSI

def show_delta(delta,level=logging.INFO) -> None:
    """
    Pretty‑print the delta a node produced.
    Works for dicts, lists, scalars – anything.
    """
    txt = pformat(delta or {}, indent=4, compact=True, width=120)
    logger.log(level, "%s", txt)
    
    if isinstance(delta, dict):
        redacted = {
            k: ("<dataframe>" if "df" in k else v)
            for k, v in delta.items()
            if k != "df_dict"
        }
        #pprint(redacted, indent=4, compact=True)

    else:  # list, str, number, None, …
        #pprint(delta, indent=4, compact=True)
        pass

def show_state(state: AgentState,level=logging.INFO) -> None:
    minimal = state.model_dump(
        exclude={"df_dict", "api_key", "openai_api_key"}
    )
    txt = pformat(minimal, indent=4, compact=True, width=120)
    logger.log(level, "%s", txt)
    #pprint(minimal, indent=4, compact=True)

def log_banner(text: str, level=logging.INFO):
    bar = "─" * len(text)
    logger.log(level, "\n%s\n%s\n%s", bar, text, bar)
    
# ──────────────────────────────────────────────────────────────────────────
# The application wrapper
# ──────────────────────────────────────────────────────────────────────────
class DataProcessingApp:
    def __init__(self, api_keys: Dict[str, str] = None) -> None:
        if api_keys:
            self.api_key = api_keys.get('deepseek', '')
            self.openai_api_key = api_keys.get('openai', '')
        else:
            self.api_key, self.openai_api_key = load_api_keys()
        
        self.df_dict = load_dataframes()

        # Build and compile the graph
        self.workflow = create_agent_workflow()

        # Optional: export the topology as DOT for offline inspection
        self._export_graph()
    # ------------------------------------------------------------------ #
    def _export_graph(self) -> None:
        """Save the workflow topology in human‑readable formats."""
        g = self.workflow.get_graph()          # ← LangGraph Graph

        # 1️⃣  Plain‑text ASCII preview (always works, no deps)
        ascii_art = g.draw_ascii()
        print("\n🖼  ASCII graph\n")
        print(ascii_art)

        # 2️⃣  Mermaid source (open in VS Code with the Mermaid preview)
        mermaid = g.draw_mermaid()
        Path("workflow.mmd").write_text(mermaid)
        print("📄  Mermaid syntax saved ➜ workflow.mmd")

        # 3️⃣  PNG image (requires `pydantic>=2.6`, `requests`, etc.)
        try:
            png_bytes = g.draw_mermaid_png(output_file_path="workflow.png")
            print("🖼  PNG graph saved   ➜ workflow.png  ({} bytes)".format(len(png_bytes)))
        except Exception as err:
            print("⚠️  Could not render PNG –", err)
    # ------------------------------------------------------------------ #
    # inside DataProcessingApp
    def process_query(self, user_query: str) -> None:
        """Run a user query through the graph – fully logged."""
        # 1️⃣ Build initial state
        try:
            state = AgentState(
                user_query=user_query,
                api_key=self.api_key,
                openai_api_key=self.openai_api_key,
                df_dict=self.df_dict,
                first_run=True  # Start with first_run=True
            )
        except Exception:
            log_banner("Failed to build AgentState", logging.ERROR)
            logger.exception("init error")
            return
        
        log_banner(f"User query → {user_query}")
        
        # 2️⃣ Main loop (allows restart after human input)
        while True:
            try:
                # Always start the workflow normally - no entry_point parameter
                event_stream = self.workflow.stream(state, stream_mode="debug")
            except Exception:
                log_banner("Failed to start event stream", logging.ERROR)
                logger.exception("stream error")
                return
                
            for ev in event_stream:
                try:
                    # node start
                    if ev["type"] == "task":
                        logger.info("▶️ %s START", ev["payload"]["name"])
                    # node end
                    elif ev["type"] == "task_result":
                        node = ev["payload"]["name"]
                        delta = to_dict_if_pairs(ev["payload"]["result"])
                        logger.info("✓ %s END — delta:", node)
                        show_delta(delta)
                        if isinstance(delta, dict):
                            state = state.model_copy(update=delta)
                        else:
                            logger.warning("%s returned %s; skipping merge", node, type(delta).__name__)
                        logger.info("current state:")
                        show_state(state)
                    # human-in-the-loop
                    if state.needs_human_input:
                        try:
                            reply = input(f"\n❓ {state.human_message}\n> ").strip()
                        except KeyboardInterrupt:
                            logger.warning("Aborted by user")
                            return
                        state = state.model_copy(
                            update=dict(human_response=reply, needs_human_input=False,first_run=False) 
                        )
                        break  # restart outer while loop with updated state
                    # workflow finished
                    elif ev["type"] == "workflow_end":
                        log_banner("Workflow finished ✅")
                        show_state(ev["payload"]["state"])
                        return
                except Exception:
                    log_banner("Exception while handling event", logging.ERROR)
                    logger.exception("event error")
                    return
            else:
                # loop ended without break (no human input) – done
                return

# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = DataProcessingApp()

    example_queries = [
         # "Please convert the date column in the EQR dataset from EST timezone to UTC timezone.", # success 2 additional input - use SKMS - use New_date column
         # "Please convert the date column in the SKMS dataset to UTC timezone.", # success 2 additional input - define original timezone - redefine column
         #"Please Join EFR and EQR based on ticker" # success first run
         'Convert the date column in SKMS table from EST to UTC timezone'
         # "Please Join EFR and EQR table based on Sales column" # success 1 additional input (define tz + redefine table) - use ticker/date column
         # "Please Join EFR and EQR table" # success 2 additional input -> use tickers column  -> use ticker column
         # "Please convert the New_date column in the EQR dataset from EST timezone to UTC timezone then join the new table with EFR based on ticker column"
    ]

    for q in example_queries:
        app.process_query(q)
