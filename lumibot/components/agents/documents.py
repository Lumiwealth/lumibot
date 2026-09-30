"""Turn any downloaded file into text and tables an agent can read.

One generic reader for every website: PDF, Word (.docx), Excel (.xlsx), CSV,
tab-separated text, HTML, JSON, plain text, and ZIP archives of any of these.
Nothing here knows about a particular site. Tables come back as pandas frames so
the caller can load them into the agent's DuckDB table tool.
"""

from __future__ import annotations

import io
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

import pandas as pd

# A ZIP is refused when its files unpack to more than this, so a tiny
# compressed "zip bomb" cannot exhaust memory in a live trading process.
MAX_UNPACKED_BYTES = 100 * 1024 * 1024
MAX_ZIP_FILES = 50
MAX_ZIP_DEPTH = 2
MAX_LINKS = 5000

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_PR = "{http://schemas.openxmlformats.org/package/2006/relationships}"


def read_document_bytes(
    content: bytes,
    *,
    name: str = "",
    content_type: str = "",
    url: str = "",
    _depth: int = 0,
) -> dict[str, Any]:
    """Return ``{"kind", "text", "tables", "files", "links"}`` for one file.

    ``tables`` is a list of ``(label, DataFrame)``. ``files`` lists the members
    of a ZIP. ``links`` lists ``{"text", "url"}`` for an HTML page.
    """
    kind = _kind(content, name=name, content_type=content_type)
    result: dict[str, Any] = {"kind": kind, "text": "", "tables": [], "files": [], "links": []}
    if kind == "pdf":
        result["text"] = _pdf_text(content)
    elif kind == "docx":
        result["text"] = _docx_text(content)
    elif kind == "xlsx":
        for sheet, frame in _xlsx_tables(content):
            result["tables"].append((sheet, frame))
        result["text"] = "\n\n".join(f"## {label}\n{frame.to_csv(sep=chr(9), index=False)}" for label, frame in result["tables"])
    elif kind == "zip":
        _read_zip(content, result, depth=_depth)
    elif kind == "html":
        result["text"], result["links"] = _html_text_and_links(content.decode("utf-8", errors="replace"), url)
    elif kind == "json":
        data = json.loads(content)
        result["text"] = json.dumps(data, indent=1, default=str)
        if isinstance(data, list) and data and all(isinstance(row, dict) for row in data):
            result["tables"].append((name or "data.json", pd.DataFrame(data)))
    elif kind in {"csv", "tsv"}:
        text = content.decode("utf-8-sig", errors="replace")
        result["text"] = text
        try:
            frame = pd.read_csv(io.StringIO(text), sep="\t" if kind == "tsv" else ",")
        except (pd.errors.ParserError, pd.errors.EmptyDataError) as error:
            # Keep the readable source, but never silently drop malformed rows
            # and present an incomplete holdings table as complete.
            result["table_error"] = str(error)
        else:
            result["tables"].append((name or f"data.{kind}", frame))
    elif kind == "text":
        result["text"] = content.decode("utf-8-sig", errors="replace")
    else:
        result["text"] = ""
        result["unsupported"] = f"Cannot read this file type ({content_type or name or 'unknown'})."
    return result


def _kind(content: bytes, *, name: str, content_type: str) -> str:
    lowered = name.lower().split("?", 1)[0]
    ctype = content_type.lower()
    if content.startswith(b"%PDF"):
        return "pdf"
    if content.startswith(b"PK\x03\x04"):
        try:
            names = set(zipfile.ZipFile(io.BytesIO(content)).namelist())
        except zipfile.BadZipFile:
            return "binary"
        if "word/document.xml" in names:
            return "docx"
        if "xl/workbook.xml" in names:
            return "xlsx"
        return "zip"
    head = content[:512].lstrip().lower()
    if "html" in ctype or head.startswith((b"<!doctype html", b"<html")):
        return "html"
    if "json" in ctype or lowered.endswith(".json"):
        return "json"
    if lowered.endswith(".csv") or "text/csv" in ctype:
        return "csv"
    if lowered.endswith(".tsv") or "tab-separated" in ctype:
        return "tsv"
    try:
        text = content[:65536].decode("utf-8-sig")
    except UnicodeDecodeError:
        return "binary"
    lines = [line for line in text.splitlines()[:6] if line.strip()]
    # Tab-separated text with the same number of columns on every line is a table.
    if len(lines) >= 2 and lines[0].count("\t") >= 1 and len({line.count("\t") for line in lines}) == 1:
        return "tsv"
    return "text"


def _pdf_text(content: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(content))
    return "\n".join((page.extract_text() or "") for page in reader.pages).replace("\x00", "")


def _docx_text(content: bytes) -> str:
    archive = zipfile.ZipFile(io.BytesIO(content))
    _check_unpacked_size(archive)
    root = ET.fromstring(archive.read("word/document.xml"))
    body = root.find(f"{_W}body")
    lines = []
    for block in list(body) if body is not None else []:
        if block.tag == f"{_W}p":
            lines.append("".join(node.text or "" for node in block.iter(f"{_W}t")))
        elif block.tag == f"{_W}tbl":
            for row in block.iter(f"{_W}tr"):
                cells = ["".join(node.text or "" for node in cell.iter(f"{_W}t")) for cell in row.findall(f"{_W}tc")]
                lines.append(" | ".join(cells))
    return "\n".join(lines)


def _column_index(ref: str) -> int:
    letters = re.match(r"[A-Z]+", ref or "A").group(0)
    index = 0
    for letter in letters:
        index = index * 26 + ord(letter) - 64
    return index - 1


def _xlsx_tables(content: bytes) -> list[tuple[str, pd.DataFrame]]:
    archive = zipfile.ZipFile(io.BytesIO(content))
    _check_unpacked_size(archive)
    names = set(archive.namelist())
    shared = []
    if "xl/sharedStrings.xml" in names:
        for item in ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(f"{_S}si"):
            shared.append("".join(node.text or "" for node in item.iter(f"{_S}t")))
    targets = {}
    if "xl/_rels/workbook.xml.rels" in names:
        for rel in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels")).findall(f"{_PR}Relationship"):
            targets[rel.get("Id")] = "xl/" + rel.get("Target", "").lstrip("/").removeprefix("xl/")
    tables = []
    for sheet in ET.fromstring(archive.read("xl/workbook.xml")).iter(f"{_S}sheet"):
        path = targets.get(sheet.get(f"{_R}id"))
        if not path or path not in names:
            continue
        rows = []
        for row in ET.fromstring(archive.read(path)).iter(f"{_S}row"):
            values: dict[int, Any] = {}
            for cell in row.findall(f"{_S}c"):
                kind = cell.get("t")
                raw = cell.find(f"{_S}v")
                if kind == "s" and raw is not None:
                    value: Any = shared[int(raw.text)]
                elif kind == "inlineStr":
                    value = "".join(node.text or "" for node in cell.iter(f"{_S}t"))
                elif raw is None:
                    continue
                else:
                    value = _number(raw.text)
                values[_column_index(cell.get("r", ""))] = value
            if values:
                rows.append([values.get(i) for i in range(max(values) + 1)])
        if rows:
            width = max(len(row) for row in rows)
            header = [str(value) if value is not None else f"column_{i + 1}" for i, value in enumerate(rows[0] + [None] * (width - len(rows[0])))]
            body = [row + [None] * (width - len(row)) for row in rows[1:]]
            tables.append((sheet.get("name") or path, pd.DataFrame(body, columns=header)))
    return tables


def _number(text: str | None) -> Any:
    if text is None:
        return None
    try:
        number = float(text)
    except ValueError:
        return text
    return int(number) if number.is_integer() else number


def _check_unpacked_size(archive: zipfile.ZipFile) -> None:
    # Office documents are ZIPs too; check before any XML is decompressed.
    if sum(info.file_size for info in archive.infolist()) > MAX_UNPACKED_BYTES:
        raise ValueError(f"Archive is too large to unpack (over {MAX_UNPACKED_BYTES // (1024 * 1024)} MB).")


def _read_zip(content: bytes, result: dict[str, Any], *, depth: int) -> None:
    archive = zipfile.ZipFile(io.BytesIO(content))
    _check_unpacked_size(archive)
    members = [info for info in archive.infolist() if not info.is_dir()]
    parts = []
    for info in members[:MAX_ZIP_FILES]:
        data = archive.read(info.filename)
        if depth >= MAX_ZIP_DEPTH and data.startswith(b"PK\x03\x04"):
            result["files"].append({"name": info.filename, "kind": "zip", "size": info.file_size, "skipped": "nested too deep"})
            continue
        inner = read_document_bytes(data, name=info.filename, _depth=depth + 1)
        entry = {"name": info.filename, "kind": inner["kind"], "size": info.file_size}
        result["files"].append(entry)
        result["tables"].extend(inner["tables"])
        result["files"].extend({**child, "name": f"{info.filename}/{child['name']}"} for child in inner["files"])
        if inner["text"]:
            parts.append(f"--- {info.filename} ---\n{inner['text']}")
    if len(members) > MAX_ZIP_FILES:
        result["files_truncated"] = True
    result["text"] = "\n\n".join(parts)


class _PageText(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.parts: list[str] = []
        self.links: list[dict[str, str]] = []
        self._skip = 0
        self._href: str | None = None
        self._link_text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript", "template"}:
            self._skip += 1
        if tag == "a":
            href = dict(attrs).get("href")
            self._href = urljoin(self.base_url, href) if href and not href.startswith(("#", "javascript:")) else None
            self._link_text = []
        if tag in {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "table", "section"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript", "template"} and self._skip:
            self._skip -= 1
        if tag == "a" and self._href:
            if len(self.links) < MAX_LINKS:
                self.links.append({"text": " ".join("".join(self._link_text).split()), "url": self._href})
            self._href = None
        if tag in {"td", "th"}:
            self.parts.append(" | ")

    def handle_data(self, data):
        if self._skip:
            return
        self.parts.append(data)
        if self._href:
            self._link_text.append(data)


def _html_text_and_links(html_text: str, base_url: str) -> tuple[str, list[dict[str, str]]]:
    parser = _PageText(base_url)
    parser.feed(html_text)
    lines = [" ".join(line.split()) for line in "".join(parser.parts).splitlines()]
    return "\n".join(line for line in lines if line), parser.links
