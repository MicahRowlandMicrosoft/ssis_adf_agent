"""Convert a Markdown report (with embedded ```mermaid``` blocks) to PDF.

Used by the engineer-facing pre/post-deployment report.  Mermaid diagrams are
rendered to PNG via ``mermaid-cli`` (``npx -y @mermaid-js/mermaid-cli``) when
available; if rendering fails, the diagram source is preserved as a code block
so the document is never lost.

The implementation is deliberately a simple subset of CommonMark — headings,
paragraphs, lists, fenced code blocks, pipe tables, horizontal rules, inline
``code``/``**bold**``/``_italic_``.  It is not a full Markdown engine; it is
tuned for the specific output of ``predeployment_report.build_predeployment_report``.
"""
from __future__ import annotations

import io
import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (
        Image,
        PageBreak,
        Paragraph,
        Preformatted,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )
    _REPORTLAB_AVAILABLE = True
except ImportError:  # pragma: no cover - import guard
    _REPORTLAB_AVAILABLE = False


# Emoji → coloured glyph mapping (reportlab doesn't ship emoji fonts).
_EMOJI_GLYPH = {
    "\U0001f7e2": ("\u25cf", "#22a522"),  # green circle
    "\U0001f535": ("\u25cf", "#2266dd"),  # blue circle
    "\U0001f7e1": ("\u25cf", "#ccaa00"),  # yellow circle
    "\U0001f7e3": ("\u25cf", "#8833bb"),  # purple circle
    "\U0001f7e0": ("\u25cf", "#dd7700"),  # orange circle
    "\u26aa":     ("\u25cb", "#999999"),  # white circle
    "\u2705":     ("\u2713", "#22a522"),  # check mark
    "\u26a0\ufe0f": ("\u26a0", "#dd7700"),  # warning sign
    "\u26a0":     ("\u26a0", "#dd7700"),
    "\u274c":     ("\u2717", "#cc2222"),  # cross mark
    "\U0001f4a1": ("*", "#ccaa00"),       # light bulb (tip)
    "\U0001f4cb": ("\u00b6", "#555555"),  # clipboard
}


def _require_reportlab() -> None:
    if not _REPORTLAB_AVAILABLE:
        raise RuntimeError(
            "reportlab is required for PDF generation. Install it with:\n"
            "    pip install reportlab"
        )


def _styles() -> dict[str, "ParagraphStyle"]:
    base = getSampleStyleSheet()
    return {
        "title":  ParagraphStyle("Title",  parent=base["Title"],    fontSize=22, leading=26, spaceAfter=12),
        "h1":     ParagraphStyle("H1",     parent=base["Heading1"], fontSize=16, spaceBefore=18, spaceAfter=6),
        "h2":     ParagraphStyle("H2",     parent=base["Heading2"], fontSize=13, spaceBefore=14, spaceAfter=4),
        "h3":     ParagraphStyle("H3",     parent=base["Heading3"], fontSize=11, spaceBefore=10, spaceAfter=4),
        "h4":     ParagraphStyle("H4",     parent=base["Heading4"], fontSize=10, spaceBefore=8,  spaceAfter=3),
        "body":   ParagraphStyle("Body",   parent=base["BodyText"], fontSize=9.5, leading=12.5),
        "mono":   ParagraphStyle("Mono",   parent=base["Code"],     fontSize=7,  leading=9, fontName="Courier"),
        "bullet": ParagraphStyle("Bullet", parent=base["BodyText"], fontSize=9.5, leading=12.5,
                                 leftIndent=18, bulletIndent=6),
        "small":  ParagraphStyle("Small",  parent=base["BodyText"], fontSize=8, leading=10, textColor=colors.grey),
    }


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _replace_emoji(text: str) -> str:
    for emoji, (glyph, colour) in _EMOJI_GLYPH.items():
        if emoji in text:
            text = text.replace(emoji, f'<font color="{colour}">{glyph}</font>')
    return text


def _inline_fmt(text: str) -> str:
    """Apply inline Markdown formatting → reportlab XML."""
    text = _esc(text)
    # Process backtick spans first — protect them from bold/italic processing
    parts = re.split(r"(`[^`]+`)", text)
    result: list[str] = []
    for part in parts:
        if part.startswith("`") and part.endswith("`"):
            inner = part[1:-1]
            result.append(f'<font name="Courier" size="8">{inner}</font>')
        else:
            # Bold: **text**
            part = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", part)
            # Italic: _text_ — only word-boundary underscores, not paths
            part = re.sub(r"(?<!\w)_([^_]+?)_(?!\w)", r"<i>\1</i>", part)
            # Markdown links [text](url) → text (url)
            part = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'\1 (<font color="#2266dd">\2</font>)', part)
            result.append(part)
    return _replace_emoji("".join(result))


def _render_mermaid_png(mermaid_code: str, *, mmdc_timeout_s: int = 180) -> bytes | None:
    """Render a mermaid diagram to PNG bytes via mermaid-cli.

    Returns None if rendering fails (caller should fall back to a code block).
    The default timeout is generous because the first ``npx -y @mermaid-js/
    mermaid-cli`` invocation downloads the package and a Chromium build, which
    can take 1\u20132 minutes on a cold cache.  Subsequent calls are typically
    sub-second.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix=".mmd", delete=False, mode="w", encoding="utf-8") as f:
            f.write(mermaid_code)
            mmd_path = Path(f.name)
        png_path = mmd_path.with_suffix(".png")
        try:
            result = subprocess.run(
                [
                    "npx", "-y", "@mermaid-js/mermaid-cli",
                    "-i", str(mmd_path),
                    "-o", str(png_path),
                    "-b", "white",
                    "-s", "2",
                ],
                capture_output=True,
                text=True,
                timeout=mmdc_timeout_s,
                shell=True,  # required on Windows so npx.cmd resolves
                stdin=subprocess.DEVNULL,
            )
            if png_path.exists() and png_path.stat().st_size > 0:
                return png_path.read_bytes()
            logger.warning(
                "mermaid-cli produced no output (rc=%s). stderr=%s",
                result.returncode, result.stderr[:200],
            )
            return None
        finally:
            mmd_path.unlink(missing_ok=True)
            png_path.unlink(missing_ok=True)
    except FileNotFoundError:
        logger.warning("npx / mermaid-cli not found on PATH; mermaid diagrams will be embedded as code.")
        return None
    except subprocess.TimeoutExpired:
        logger.warning("mermaid-cli timed out after %ds.", mmdc_timeout_s)
        return None
    except Exception as exc:
        logger.warning("mermaid render failed: %s", exc)
        return None


def _scaled_image(png_bytes: bytes, *, max_w_in: float = 6.5, max_h_in: float = 7.0) -> "Image":
    img_reader = ImageReader(io.BytesIO(png_bytes))
    iw, ih = img_reader.getSize()
    max_w = max_w_in * inch
    max_h = max_h_in * inch
    scale = min(max_w / iw, max_h / ih, 1.0)
    # Reportlab needs an on-disk path or a file-like; use a temp file pointer
    # because Image() reads lazily.
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    tmp.write(png_bytes)
    tmp.close()
    return Image(tmp.name, width=iw * scale, height=ih * scale)


def markdown_to_pdf(
    markdown: str,
    output_pdf: str | Path,
    *,
    title: str = "Report",
    render_mermaid: bool = True,
) -> dict[str, Any]:
    """Convert ``markdown`` text to a PDF at ``output_pdf``.

    Returns a summary dict with: ``output_pdf``, ``size_bytes``, ``mermaid_count``,
    ``mermaid_rendered`` (number of diagrams successfully rendered to images),
    ``mermaid_fallback`` (number embedded as code blocks because rendering failed).
    """
    _require_reportlab()
    out_path = Path(output_pdf).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    S = _styles()

    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=LETTER,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.5 * inch,
        title=title,
    )

    story: list[Any] = []
    in_code = False
    code_lang = ""
    code_lines: list[str] = []
    in_table = False
    table_rows: list[list[str]] = []
    mermaid_count = 0
    mermaid_rendered = 0
    mermaid_fallback = 0

    def flush_table() -> None:
        nonlocal table_rows
        if not table_rows:
            return
        max_cols = max(len(r) for r in table_rows)
        for r in table_rows:
            while len(r) < max_cols:
                r.append("")
        data = [
            [Paragraph(_inline_fmt(c), S["small"]) for c in row]
            for row in table_rows
        ]
        avail = 7.0 * inch
        col_w = [avail / max_cols] * max_cols
        tbl = Table(data, colWidths=col_w, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8),
            ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor("#e8e8f0")),
            ("LINEBELOW",     (0, 0), (-1, 0), 0.75, colors.HexColor("#888")),
            ("LINEBELOW",     (0, 1), (-1, -1), 0.25, colors.HexColor("#ddd")),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING",   (0, 0), (-1, -1), 3),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 3),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(Spacer(1, 4))
        story.append(tbl)
        story.append(Spacer(1, 4))
        table_rows = []

    for line in markdown.splitlines():
        # ── Fenced code blocks ──────────────────────────────────────────
        if line.lstrip().startswith("```") and not in_code:
            in_code = True
            code_lang = line.lstrip().lstrip("`").strip().lower()
            code_lines = []
            continue
        if line.lstrip().startswith("```") and in_code:
            in_code = False
            code_text = "\n".join(code_lines)
            if code_text.strip():
                if code_lang == "mermaid":
                    mermaid_count += 1
                    png = _render_mermaid_png(code_text) if render_mermaid else None
                    if png:
                        mermaid_rendered += 1
                        story.append(Spacer(1, 6))
                        story.append(_scaled_image(png))
                        story.append(Spacer(1, 6))
                    else:
                        mermaid_fallback += 1
                        story.append(Spacer(1, 4))
                        story.append(Paragraph("<i>(mermaid diagram — rendering unavailable; source preserved below)</i>", S["small"]))
                        story.append(Preformatted(_esc(code_text), S["mono"]))
                        story.append(Spacer(1, 4))
                else:
                    story.append(Spacer(1, 4))
                    story.append(Preformatted(_esc(code_text), S["mono"]))
                    story.append(Spacer(1, 4))
            continue
        if in_code:
            code_lines.append(line)
            continue

        # ── Pipe tables ─────────────────────────────────────────────────
        if "|" in line and line.strip().startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if all(set(c) <= set("-: ") for c in cells):  # separator row
                continue
            if not in_table:
                in_table = True
                table_rows = []
            table_rows.append(cells)
            continue
        if in_table:
            in_table = False
            flush_table()

        stripped = line.strip()

        # ── Headings ────────────────────────────────────────────────────
        if stripped.startswith("##### "):
            story.append(Paragraph(_inline_fmt(stripped[6:]), S["h4"]))
            continue
        if stripped.startswith("#### "):
            story.append(Paragraph(_inline_fmt(stripped[5:]), S["h3"]))
            continue
        if stripped.startswith("### "):
            story.append(Paragraph(_inline_fmt(stripped[4:]), S["h2"]))
            continue
        if stripped.startswith("## "):
            story.append(PageBreak())
            story.append(Paragraph(_inline_fmt(stripped[3:]), S["h1"]))
            continue
        if stripped.startswith("# "):
            story.append(Paragraph(_inline_fmt(stripped[2:]), S["title"]))
            continue

        # ── Horizontal rule ─────────────────────────────────────────────
        if stripped == "---":
            story.append(Spacer(1, 6))
            continue

        # ── Bullet ──────────────────────────────────────────────────────
        if stripped.startswith("- ") or stripped.startswith("* "):
            story.append(Paragraph(_inline_fmt(stripped[2:]), S["bullet"], bulletText="\u2022"))
            continue

        # ── Numbered list ───────────────────────────────────────────────
        m = re.match(r"^(\d+)\.\s+(.+)", stripped)
        if m:
            story.append(Paragraph(_inline_fmt(m.group(2)), S["bullet"], bulletText=f"{m.group(1)}."))
            continue

        # ── Body text ───────────────────────────────────────────────────
        if stripped:
            story.append(Paragraph(_inline_fmt(stripped), S["body"]))

    if in_table:
        flush_table()

    doc.build(story)
    size = out_path.stat().st_size
    return {
        "output_pdf": str(out_path),
        "size_bytes": size,
        "mermaid_count": mermaid_count,
        "mermaid_rendered": mermaid_rendered,
        "mermaid_fallback": mermaid_fallback,
    }
