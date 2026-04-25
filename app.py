from __future__ import annotations

import os

from flask import Flask, redirect
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

app = Flask(__name__)


@app.route("/")
def root():
    return redirect("/analyzer/")


def build_dispatcher(base_wsgi_app):
    try:
        from .analyzer.app import app as analyzer_app
        from .pipeline.app import app as pipeline_app
    except ImportError:
        from analyzer.app import app as analyzer_app
        from pipeline.app import app as pipeline_app

    return DispatcherMiddleware(
        base_wsgi_app,
        {
            "/analyzer": analyzer_app,
            "/pipeline": pipeline_app,
        },
    )


app.wsgi_app = build_dispatcher(app.wsgi_app)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    run_simple("127.0.0.1", port, app, use_debugger=True, use_reloader=True)
