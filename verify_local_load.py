#!/usr/bin/env python3
"""Quick local check that the Digital CSIC plugin can be imported."""

import sys


def _load_entry_points():
    try:
        from importlib.metadata import entry_points
    except Exception:  # pragma: no cover - Python < 3.8 fallback
        from importlib_metadata import entry_points  # type: ignore
    return entry_points()


def main() -> int:
    print("Python executable:", sys.executable)

    try:
        from fair_eva.plugin.digital_csic import Plugin  # noqa: F401
        print("OK: imported fair_eva.plugin.digital_csic.Plugin")
    except Exception as exc:
        print("ERROR: cannot import Plugin:", repr(exc))
        return 1

    try:
        eps = _load_entry_points()
        group = eps.select(group="fair_eva.plugin")
        matches = [ep for ep in group if ep.name == "digital_csic"]
        if matches:
            print("OK: entry point fair_eva.plugin:digital_csic is registered")
            print("Entry point target:", matches[0].value)
        else:
            print("WARN: entry point fair_eva.plugin:digital_csic not found")
    except Exception as exc:
        print("WARN: could not inspect entry points:", repr(exc))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
