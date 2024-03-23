from fastapi import FastAPI

import src.modules as routes
import importlib
from src.config import get_settings
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from src.connectors.sqlalchemy import Base, engine
from src.modules.dag.model import DAG


settings = get_settings()
DAG.metadata.create_all(bind=engine)
app = FastAPI(
    swagger_ui_parameters={
        "syntaxHighlight.theme": "obsidian",
        "docExpansion": "list",
        "filter": True,
    }
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allow_origins,
    allow_credentials=True,
    allow_methods=settings.allow_methods,
    allow_headers=settings.allow_headers,
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allow_hosts)

for route in routes.modules:
    module = importlib.import_module(f"src.modules.{route}.routes")
    app.include_router(module.router, prefix=settings.prefix)
