"""Stream the official Kurdish Wikipedia XML dump into the corpus store."""
from __future__ import annotations

import bz2
import hashlib
import os
import re
import tempfile
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

from store import Store

DUMP_URL = "https://dumps.wikimedia.org/kuwiki/latest/kuwiki-latest-pages-articles.xml.bz2"
CHECKPOINT_KEY = "kuwiki_import_v1"
BATCH_SIZE = 256
RAW_URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>]+", re.IGNORECASE)


def _snapshot_last_modified() -> str:
    request = urllib.request.Request(DUMP_URL, method="HEAD", headers={"User-Agent": "kurdish-corpus/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        raise RuntimeError("Wikimedia dump response has no Last-Modified header; cannot pin a resumable snapshot")
    return last_modified


def _ensure_dump(data_dir: Path, last_modified: str) -> Path:
    # Keep a versioned immutable local snapshot so a restarted run uses the same dump.
    key = hashlib.sha256(last_modified.encode("utf-8")).hexdigest()[:16]
    dump = data_dir / f"kuwiki-{key}.xml.bz2"
    if dump.is_file():
        return dump
    fd, tmp_name = tempfile.mkstemp(prefix="kuwiki-", suffix=".part", dir=data_dir)
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        request = urllib.request.Request(DUMP_URL, headers={"User-Agent": "kurdish-corpus/1.0"})
        with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as output:
            received_last_modified = response.headers.get("Last-Modified")
            if received_last_modified and received_last_modified != last_modified:
                raise RuntimeError("Wikimedia latest dump changed between HEAD and download; retry the import")
            while block := response.read(1024 * 1024):
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        # Validate the bzip2 stream before making the snapshot visible.
        with bz2.open(tmp, "rb") as compressed:
            compressed.read(1)
        tmp.replace(dump)
    finally:
        tmp.unlink(missing_ok=True)
    return dump


def _latin_kurdish(text: str) -> bool:
    letters = [char for char in text if char.isalpha()]
    if len(letters) < 50:
        return False
    latin = sum("LATIN" in unicodedata.name(char, "") for char in letters)
    return latin / len(letters) >= 0.6


def _revision_text(revision: ET.Element) -> str:
    """Read legacy revision/text or the modern main-slot text element."""
    text = revision.find("{*}text")
    if text is not None:
        return text.text or ""
    slots = revision.find("{*}slots")
    if slots is None:
        return ""
    for slot in slots.findall("{*}slot"):
        if slot.get("role") == "main" or slot.get("role") is None:
            text = slot.find("{*}text")
            if text is not None:
                return text.text or ""
    return ""


def import_wiki(store: Store, data_dir: str | os.PathLike[str]) -> int:
    """Download and import namespace-zero, nonredirect, Latin-script kuwiki pages."""
    try:
        import mwparserfromhell
    except ImportError as exc:
        raise RuntimeError("Wikipedia import requires `pip install mwparserfromhell`") from exc

    directory = Path(data_dir)
    directory.mkdir(parents=True, exist_ok=True)
    last_modified = _snapshot_last_modified()
    checkpoint = store.get_checkpoint(CHECKPOINT_KEY, {})
    if checkpoint.get("last_modified") == last_modified and checkpoint.get("complete") is True:
        return 0
    dump = _ensure_dump(directory, last_modified)
    last_page_id = int(checkpoint.get("last_page_id", 0)) if checkpoint.get("last_modified") == last_modified else 0
    pending_page_id = last_page_id
    max_page_id = last_page_id
    imported = 0
    processed = 0

    with bz2.open(dump, "rb") as compressed:
        context = ET.iterparse(compressed, events=("start", "end"))
        _, root = next(context)
        for event, page in context:
            if event != "end" or page.tag.rsplit("}", 1)[-1] != "page":
                continue
            ns = page.findtext("{*}ns", default="")
            page_id = int(page.findtext("{*}id", default="0") or 0)
            max_page_id = max(max_page_id, page_id)
            if page_id > last_page_id and ns == "0" and page.find("{*}redirect") is None:
                title = page.findtext("{*}title", default="")
                revision = page.find("{*}revision")
                if revision is not None:
                    raw_text = _revision_text(revision)
                    timestamp = revision.findtext("{*}timestamp", default="") or ""
                    text = RAW_URL_RE.sub(" ", mwparserfromhell.parse(raw_text).strip_code()).strip()
                    if _latin_kurdish(text):
                        url = "https://ku.wikipedia.org/wiki/" + quote(title.replace(" ", "_"), safe="()_,-")
                        if store.add(text, url, "Wikipedia", title=title, date=timestamp,
                                     upstream_source="Wikimedia kuwiki dump", source_id=str(page_id),
                                     license="CC BY-SA 4.0", commit=False):
                            imported += 1
                pending_page_id = page_id
                processed += 1
                if processed % BATCH_SIZE == 0:
                    store.set_checkpoint(CHECKPOINT_KEY, {"last_modified": last_modified,
                                                          "last_page_id": pending_page_id})
                    print(f"kuwiki pages scanned: {processed:,}; added: {imported:,}", flush=True)
            root.clear()

    store.set_checkpoint(CHECKPOINT_KEY, {"last_modified": last_modified,
                                          "last_page_id": max(max_page_id, pending_page_id),
                                          "complete": True})
    return imported


def main() -> None:
    data_dir = Path(os.environ.get("DATA_DIR", "data")).expanduser().resolve()
    with Store(data_dir) as store:
        count = import_wiki(store, data_dir)
        print(f"Wikipedia pages added: {count:,}", flush=True)


if __name__ == "__main__":
    main()
