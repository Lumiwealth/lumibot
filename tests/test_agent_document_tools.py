"""read_document: one generic tool that reads any file an agent finds on the web.

Rob, 2026-09-29: no tool built for one website. An agent given a URL must be
able to read whatever is there: a PDF, a Word or Excel file, a CSV, or a ZIP of
them. Tables go into the agent's table tool so it can filter and add up rows
instead of doing the math in its head. The Nancy Pelosi example reads the House
Clerk's yearly ZIP index and PDF reports this way.
"""

import io
import zipfile
from types import SimpleNamespace

import httpx
import pytest

from lumibot.components.agents.builtins import BuiltinTools
from lumibot.components.agents.documents import read_document_bytes
from lumibot.components.agents.duckdb_tools import DuckDBQueryLayer
from lumibot.components.agents.manager import NETWORK_TOOL_NAMES
from lumibot.components.agents.web_tools import WebClient

_INDEX_TSV = (
    "Prefix\tLast\tFirst\tSuffix\tFilingType\tStateDst\tYear\tFilingDate\tDocID\n"
    "Hon.\tPelosi\tNancy\t\tO\tCA11\t2025\t5/15/2026\t10075701\n"
    "Hon.\tPelosi\tNancy\t\tP\tCA11\t2025\t7/9/2025\t20030630\n"
    "Hon.\tSmith\tAnn\t\tP\tCA12\t2025\t8/1/2025\t20031111\n"
)
_LONG_TEXT = "\n".join(f"line {i} {'Pelosi' if i % 10 == 0 else 'other'}" for i in range(100))


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _docx() -> bytes:
    w = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    body = (
        f'<w:document xmlns:w="{w}"><w:body>'
        "<w:p><w:r><w:t>Quarterly letter</w:t></w:r></w:p>"
        "<w:tbl><w:tr><w:tc><w:p><w:r><w:t>Ticker</w:t></w:r></w:p></w:tc>"
        "<w:tc><w:p><w:r><w:t>Weight</w:t></w:r></w:p></w:tc></w:tr>"
        "<w:tr><w:tc><w:p><w:r><w:t>NVDA</w:t></w:r></w:p></w:tc>"
        "<w:tc><w:p><w:r><w:t>40%</w:t></w:r></w:p></w:tc></w:tr></w:tbl>"
        "</w:body></w:document>"
    )
    return _zip({"[Content_Types].xml": b"<Types/>", "word/document.xml": body.encode()})


def _xlsx() -> bytes:
    main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    workbook = (
        f'<workbook xmlns="{main}" xmlns:r="{rel}"><sheets>'
        '<sheet name="Holdings" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    rels = (
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Target="worksheets/sheet1.xml" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/>'
        "</Relationships>"
    )
    shared = f'<sst xmlns="{main}"><si><t>Ticker</t></si><si><t>Shares</t></si><si><t>AAPL</t></si></sst>'
    sheet = (
        f'<worksheet xmlns="{main}"><sheetData>'
        '<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>'
        '<row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>150</v></c></row>'
        '<row r="3"><c r="A3" t="inlineStr"><is><t>MSFT</t></is></c><c r="B3"><v>75</v></c></row>'
        "</sheetData></worksheet>"
    )
    return _zip(
        {
            "[Content_Types].xml": b"<Types/>",
            "xl/workbook.xml": workbook.encode(),
            "xl/_rels/workbook.xml.rels": rels.encode(),
            "xl/sharedStrings.xml": shared.encode(),
            "xl/worksheets/sheet1.xml": sheet.encode(),
        }
    )


def _pdf(text: str) -> bytes:
    """A one-page PDF whose page text is ``text`` (Helvetica, no compression)."""
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def test_pdf_becomes_text():
    result = read_document_bytes(_pdf("Purchased 10,000 shares of NVDA"), name="report.pdf")
    assert result["kind"] == "pdf"
    assert "Purchased 10,000 shares of NVDA" in result["text"]


def test_zip_lists_every_file_and_reads_the_tab_separated_index_as_a_table():
    archive = _zip({"2025FD.txt": _INDEX_TSV.encode(), "notes/readme.txt": b"Index of filings"})
    result = read_document_bytes(archive, name="2025FD.ZIP")

    assert result["kind"] == "zip"
    assert [entry["name"] for entry in result["files"]] == ["2025FD.txt", "notes/readme.txt"]
    assert "Index of filings" in result["text"]
    frame = dict(result["tables"])["2025FD.txt"]
    assert list(frame.columns)[:2] == ["Prefix", "Last"]
    assert sorted(frame["Last"]) == ["Pelosi", "Pelosi", "Smith"]


def test_word_file_becomes_text_with_its_table_rows():
    result = read_document_bytes(_docx(), name="letter.docx")
    assert result["kind"] == "docx"
    assert "Quarterly letter" in result["text"]
    assert "NVDA | 40%" in result["text"]


def test_excel_file_becomes_one_table_per_sheet():
    result = read_document_bytes(_xlsx(), name="book.xlsx")
    assert result["kind"] == "xlsx"
    frame = dict(result["tables"])["Holdings"]
    assert frame.to_dict(orient="records") == [
        {"Ticker": "AAPL", "Shares": 150},
        {"Ticker": "MSFT", "Shares": 75},
    ]


def test_csv_becomes_a_table():
    result = read_document_bytes(b"ticker,shares\nAAPL,10\nMSFT,5\n", name="holdings.csv", content_type="text/csv")
    assert result["kind"] == "csv"
    assert dict(result["tables"])["holdings.csv"]["shares"].sum() == 15


def test_malformed_csv_keeps_text_without_publishing_a_partial_table():
    content = b"ticker,shares\nAAPL,10\nMSFT,5,unexpected\n"
    result = read_document_bytes(content, name="holdings.csv")
    assert result["text"] == content.decode()
    assert result["tables"] == []
    assert result["table_error"]


def test_html_page_becomes_text_and_absolute_links():
    page = b"""<html><head><script>var x=1;</script></head><body><h1>Reports</h1>
      <a href="/public_disc/ptr-pdfs/2026/20033725.pdf">Pelosi PTR</a></body></html>"""
    result = read_document_bytes(page, name="search", content_type="text/html", url="https://clerk.example/search")
    assert result["kind"] == "html"
    assert "Reports" in result["text"] and "var x" not in result["text"]
    assert result["links"] == [
        {"text": "Pelosi PTR", "url": "https://clerk.example/public_disc/ptr-pdfs/2026/20033725.pdf"}
    ]


def test_zip_bomb_is_refused():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("huge.txt", b"0" * (120 * 1024 * 1024))
    with pytest.raises(ValueError, match="too large"):
        read_document_bytes(buffer.getvalue(), name="bomb.zip")


@pytest.mark.parametrize("factory,name", [(_docx, "letter.docx"), (_xlsx, "book.xlsx")])
def test_office_archives_obey_the_unpacked_size_limit(factory, name, monkeypatch):
    from lumibot.components.agents import documents

    monkeypatch.setattr(documents, "MAX_UNPACKED_BYTES", 100)
    with pytest.raises(ValueError, match="too large"):
        read_document_bytes(factory(), name=name)


def test_document_tables_with_the_same_label_remain_independently_queryable():
    import pandas as pd

    layer = DuckDBQueryLayer(SimpleNamespace())
    first = layer.register_document_table("Holdings", pd.DataFrame({"shares": [10]}), source="https://example.com/a.xlsx")
    second = layer.register_document_table("Holdings", pd.DataFrame({"shares": [20]}), source="https://example.com/b.xlsx")
    assert first["table_name"] != second["table_name"]
    assert layer.query(sql=f"SELECT shares FROM {first['table_name']}")["rows"] == [{"shares": 10}]
    assert layer.query(sql=f"SELECT shares FROM {second['table_name']}")["rows"] == [{"shares": 20}]


def _bound_tool():
    archive = _zip({"2025FD.txt": _INDEX_TSV.encode()})

    def handler(request):
        if request.url.path.endswith(".ZIP"):
            return httpx.Response(200, content=archive, headers={"content-type": "application/x-zip-compressed"})
        return httpx.Response(200, content=_LONG_TEXT.encode(), headers={"content-type": "text/plain"})

    strategy = SimpleNamespace()
    strategy._agent_web_client = WebClient(
        transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"]
    )
    manager = SimpleNamespace(duckdb=DuckDBQueryLayer(strategy))
    return BuiltinTools.web.read_document().binder(strategy, manager), manager


def test_read_document_tool_loads_tables_for_the_table_tool():
    tool, manager = _bound_tool()
    result = tool.function(url="https://clerk.example/public_disc/financial-pdfs/2025FD.ZIP")

    assert result["ok"] is True and result["kind"] == "zip"
    table = result["tables"][0]
    assert table["row_count"] == 3
    assert "FilingDate" in table["columns"]
    rows = manager.duckdb.query(
        sql=f"SELECT DocID FROM {table['table_name']} WHERE Last = 'Pelosi' AND FilingType = 'O'"
    )["rows"]
    assert [row["DocID"] for row in rows] == [10075701]


def test_read_document_tool_finds_lines_and_pages_through_long_text():
    tool, _ = _bound_tool()
    found = tool.function(url="https://clerk.example/long.txt", find="pelosi")
    assert found["text"].splitlines() == [f"line {i} Pelosi" for i in range(0, 100, 10)]

    first = tool.function(url="https://clerk.example/long.txt", max_chars=50)
    assert len(first["text"]) == 50 and first["next_start"] == 50
    second = tool.function(url="https://clerk.example/long.txt", start=50, max_chars=50)
    assert first["text"] + second["text"] == _LONG_TEXT[:100]
    last = tool.function(url="https://clerk.example/long.txt", start=len(_LONG_TEXT) - 10)
    assert last["next_start"] is None


def test_read_document_is_a_network_tool_listed_with_the_builtins():
    assert "read_document" in NETWORK_TOOL_NAMES
    assert "read_document" in {tool.name for tool in BuiltinTools.all()}


def test_read_document_results_fit_the_agent_tool_result_limit():
    # The agent runtime cuts any tool result over 4,000 characters down to its
    # head and tail, so a long default page silently hid the middle of a report.
    import json

    from lumibot.components.agents.runtime import _prune_tool_response_for_context_window

    links = "".join(f'<a href="/r/{i}.pdf">Report {i}</a>' for i in range(400))
    page = f"<html><body><p>{'word ' * 3000}</p>{links}</body></html>".encode()

    def handler(request):
        return httpx.Response(200, content=page, headers={"content-type": "text/html"})

    strategy = SimpleNamespace()
    strategy._agent_web_client = WebClient(
        transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"]
    )
    tool = BuiltinTools.web.read_document().binder(strategy, SimpleNamespace(duckdb=DuckDBQueryLayer(strategy)))

    result = tool.function(url="https://clerk.example/list")
    assert _prune_tool_response_for_context_window(result, tool_name="read_document") is None, len(json.dumps(result))
    assert result["next_start"] and result["links_total"] == 400

    found = tool.function(url="https://clerk.example/list", find="Report 12")
    assert [link["text"] for link in found["links"]] == [f"Report {n}" for n in (12, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129)]


def test_the_table_tool_is_a_calculator_for_portfolio_weights():
    # A portfolio agent once scaled stocks 1,000x too small doing the math in its head.
    strategy = SimpleNamespace()
    tool = BuiltinTools.duckdb.query().binder(strategy, SimpleNamespace(duckdb=DuckDBQueryLayer(strategy)))
    assert "VALUES" in tool.description and "calculator" in tool.description.lower()

    rows = tool.function(
        sql="SELECT ticker, ROUND(100 * mid / SUM(mid) OVER (), 2) AS pct "
        "FROM (VALUES ('AAPL', 15000000.5), ('GOOGL', 15000000.5), ('AB', 3000000.5)) AS h(ticker, mid) ORDER BY ticker"
    )["rows"]
    assert rows == [{"ticker": "AAPL", "pct": 45.45}, {"ticker": "AB", "pct": 9.09}, {"ticker": "GOOGL", "pct": 45.45}]
