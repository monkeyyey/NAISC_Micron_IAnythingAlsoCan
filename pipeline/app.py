from __future__ import annotations

import importlib.util
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PIPELINE_DIR.parent
SOURCE_APP = ROOT_DIR / "imported_app" / "AI Singapore" / "format_parsing" / "app.py"


def load_pipeline_app():
    spec = importlib.util.spec_from_file_location("structured_pipeline_app", SOURCE_APP)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load pipeline app from {SOURCE_APP}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    loaded = getattr(module, "app", None)
    if loaded is None:
        raise RuntimeError(f"No Flask app found in {SOURCE_APP}")
    return loaded


app = load_pipeline_app()


if __name__ == "__main__":
    app.run(debug=True, port=5051)
