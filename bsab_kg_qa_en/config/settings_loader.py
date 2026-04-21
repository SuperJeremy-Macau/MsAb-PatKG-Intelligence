# bsab_kg_qa_en/config/settings_loader.py
from __future__ import annotations
from typing import Any, Dict
import os


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s


def _strip_inline_comment(v: str) -> str:
    """
    Remove inline comments that start with #, but only when # is not inside quotes.
    Example:
      database: "neo4j-xxx" # comment   -> database: "neo4j-xxx"
      text: "a#b" # comment             -> text: "a#b"
    """
    v = v.rstrip()
    in_quote = False
    quote_ch = ""
    out = []
    for ch in v:
        if ch in ("'", '"'):
            if not in_quote:
                in_quote = True
                quote_ch = ch
            elif quote_ch == ch:
                in_quote = False
        if ch == "#" and not in_quote:
            break
        out.append(ch)
    return "".join(out).strip()


def _parse_scalar(v: str) -> Any:
    v = _strip_inline_comment(v)
    v = _strip_quotes(v)

    if v.lower() in ("true", "false"):
        return v.lower() == "true"

    # int
    try:
        if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
            return int(v)
    except Exception:
        pass

    # float
    try:
        if "." in v:
            return float(v)
    except Exception:
        pass

    return v


def _merge_settings(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def load_settings(path: str) -> Dict[str, Any]:
    """
    Minimal YAML loader for this project:
    - Supports top-level key: value
    - Supports one-level nested sections:
        neo4j:
          uri: "..."
    - Supports inline # comments safely (outside quotes)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"settings file not found: {path}")

    root: Dict[str, Any] = {}
    current_section: str | None = None

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip() or line.strip().startswith("#"):
                continue

            if not line.startswith(" "):  # top-level
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                k = k.strip()
                v = v.strip()
                v = _strip_inline_comment(v)

                if v == "":
                    root[k] = {}
                    current_section = k
                else:
                    root[k] = _parse_scalar(v)
                    current_section = None
            else:
                if current_section is None:
                    continue
                stripped = line.strip()
                if ":" not in stripped:
                    continue
                k, v = stripped.split(":", 1)
                k = k.strip()
                v = v.strip()
                root[current_section][k] = _parse_scalar(v)

    local_path = os.path.join(
        os.path.dirname(path),
        f"{os.path.splitext(os.path.basename(path))[0]}.local{os.path.splitext(path)[1]}",
    )
    if os.path.exists(local_path):
        root = _merge_settings(root, load_settings(local_path))

    return root
