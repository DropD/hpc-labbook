"""CLI typer app."""

from __future__ import annotations

import typer

__all__ = ["add_site", "app", "auth_site"]


app = typer.Typer(name="hpclb")
app.add_typer(add_site := typer.Typer(), name="add-site")
app.add_typer(auth_site := typer.Typer(), name="auth-site")
