"""Resumable, streaming Kurmanji corpus importer. Run: python importer.py"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from store import Store, migrate_store

HF_REPO = "kurdish-tech/KurdishCorpus-clean"
HF_REVISION = "5c446d5ff96bb03aff48dbab83731935f17987fd"
HF_SNAPSHOT_BYTES = 7_681_828_199
HF_URL = f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/corpus_v2_open_release.jsonl"
INTERDIALECT_REVISION = "a27afa395992c388c777bd0b1dbc0e412484550a"
INTERDIALECT_FILES = {
    "CKB-KMR/CKB-KMR_kmr_no_tag.txt": f"https://raw.githubusercontent.com/KurdishBLARK/InterdialectCorpus/{INTERDIALECT_REVISION}/CKB-KMR/CKB-KMR_kmr_no_tag.txt",
    "KMR-ENG/KMR-ENG.KMR_no_tag.txt": f"https://raw.githubusercontent.com/KurdishBLARK/InterdialectCorpus/{INTERDIALECT_REVISION}/KMR-ENG/KMR-ENG.KMR_no_tag.txt",
}
BIANET_API = "https://opus.nlpl.eu/opusapi/?corpus=Bianet"
CHECKPOINT_INTERVAL = 1000


def request(url: str, headers: dict | None = None, timeout: int = 120):
    request_obj = urllib.request.Request(url, headers={"User-Agent": "kurdish-corpus-importer/1.0", **(headers or {})})
    return urllib.request.urlopen(request_obj, timeout=timeout)


def cache_download(url: str, dest: Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=dest.name + ".", suffix=".tmp", dir=dest.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        with request(url) as response, temp_path.open("wb") as out:
            shutil.copyfileobj(response, out, length=1024 * 1024)
        os.replace(temp_path, dest)
    finally:
        temp_path.unlink(missing_ok=True)


def import_hf(store: Store, data_dir: Path) -> None:
    """Prefer the complete predownloaded source, otherwise range-resume HF JSONL."""
    local_source = data_dir / "corpus_v2_open_release.jsonl"
    checkpoint_key = "hf:full-jsonl"
    state = store.get_checkpoint(checkpoint_key, {"offset": 0, "records": 0})
    offset, records = int(state["offset"]), int(state["records"])
    if offset == HF_SNAPSHOT_BYTES:
        return
    if local_source.is_file():
        if local_source.stat().st_size != HF_SNAPSHOT_BYTES:
            raise ValueError("Local HF snapshot has an unexpected size")
        stream = local_source.open("rb")
        stream.seek(offset)
    else:
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        stream = request(HF_URL, headers=headers)
        if offset and stream.status != 206:
            stream.close()
            raise RuntimeError("HF did not honor byte-range resume; refusing to restart silently")
    with stream:
        since_commit = 0
        for line in stream:
            offset += len(line)
            records += 1
            row = json.loads(line)
            if row.get("lid_label") == "kmr_Latn":
                store.add(
                    row.get("text", ""), row.get("url") or "", "HuggingFace",
                    row.get("title") or "", row.get("date") or "",
                    upstream_source=row.get("source") or "",
                    source_id=row.get("id") or "",
                    license=row.get("license") or "",
                    commit=False,
                )
                since_commit += 1
            if since_commit >= 256:
                store.commit()
                since_commit = 0
            if records % CHECKPOINT_INTERVAL == 0:
                store.set_checkpoint(checkpoint_key, {"offset": offset, "records": records})
                if records % 100_000 == 0:
                    print(f"HF rows: {records:,}", flush=True)
    if offset != HF_SNAPSHOT_BYTES:
        raise OSError(f"HF snapshot ended at byte {offset}; expected {HF_SNAPSHOT_BYTES}")
    store.set_checkpoint(checkpoint_key, {"offset": offset, "records": records})


def import_pewan(store: Store, data_dir: Path) -> None:
    """Read the nested Kurmanji archive and checkpoint by archive entry index."""
    archive = data_dir / "Pewan.zip"
    if not archive.is_file():
        return
    state_key = "pewan:nested-zip"
    state = store.get_checkpoint(state_key, {"entry_index": 0})
    completed_index = int(state["entry_index"])
    with zipfile.ZipFile(archive) as outer:
        nested = next((i for i in outer.infolist() if i.filename.lower().endswith("pewan/corpora/kurmanji.zip")), None)
        if nested is None:
            raise RuntimeError("Pewan archive lacks the expected Pewan/Corpora/Kurmanji.zip")
        fd, nested_name = tempfile.mkstemp(prefix="pewan-inner-", suffix=".zip", dir=data_dir)
        os.close(fd)
        nested_path = Path(nested_name)
        try:
            with outer.open(nested) as source, nested_path.open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            with zipfile.ZipFile(nested_path) as inner:
                entries = inner.infolist()
                for index, info in enumerate(entries):
                    if index < completed_index:
                        continue
                    if not info.is_dir() and info.filename.lower().endswith(".txt"):
                        with inner.open(info) as member:
                            text = member.read().decode("utf-8", "strict")
                        store.add(
                            text, f"archive://Pewan.zip/{nested.filename}!/{info.filename}", "Pewan",
                            title=Path(info.filename).stem,
                            upstream_source="Pewan/Corpora/Kurmanji.zip",
                            source_id=info.filename,
                            license="",
                            commit=False,
                        )
                    if (index + 1) % 100 == 0:
                        store.set_checkpoint(state_key, {"entry_index": index + 1})
                store.set_checkpoint(state_key, {"entry_index": len(entries)})
        finally:
            nested_path.unlink(missing_ok=True)


def import_bianet(store: Store, data_dir: Path) -> None:
    """Resolve the current Bianet Kurdish monolingual file from the OPUS API."""
    payload = json.load(request(BIANET_API, timeout=60))
    options = [c for c in payload.get("corpora", [])
               if c.get("corpus") == "Bianet" and c.get("source") == "ku"
               and c.get("preprocessing") == "mono" and str(c.get("url", "")).endswith("/mono/ku.txt.gz")]
    if not options:
        raise RuntimeError("OPUS API did not return Bianet Kurdish mono/ku.txt.gz")
    item = max(options, key=lambda c: c.get("version", ""))
    source_url = item["url"]
    cache = data_dir / "sources" / "bianet-ku.txt.gz"
    cache_download(source_url, cache)
    key = f"bianet:{source_url}"
    state = store.get_checkpoint(key, {"line": 0})
    start_line = int(state["line"])
    last_line = start_line - 1
    with gzip.open(cache, "rt", encoding="utf-8", errors="replace") as stream:
        for line_number, sentence in enumerate(stream):
            last_line = line_number
            if line_number < start_line:
                continue
            sentence = sentence.strip()
            if sentence:
                store.add(sentence, source_url, "Bianet", upstream_source="OPUS Bianet v1 mono/ku",
                          source_id=f"Bianet:mono:ku:{line_number}", license="CC-BY-SA-4.0", commit=False)
            if (line_number + 1) % CHECKPOINT_INTERVAL == 0:
                store.set_checkpoint(key, {"line": line_number + 1})
    store.set_checkpoint(key, {"line": last_line + 1})


def import_interdialect(store: Store, data_dir: Path) -> None:
    """Stream the Kurmanji side of the two aligned plain-text files."""
    for relative, url in INTERDIALECT_FILES.items():
        local = data_dir / "sources" / Path(relative).name
        cache_download(url, local)
        key = f"interdialect:{relative}"
        state = store.get_checkpoint(key, {"line": 0})
        start_line = int(state["line"])
        last_line = start_line - 1
        with local.open("r", encoding="utf-8", errors="strict") as stream:
            for line_number, sentence in enumerate(stream):
                last_line = line_number
                if line_number < start_line:
                    continue
                sentence = sentence.strip()
                if sentence:
                    store.add(sentence, url, "Interdialect", upstream_source=f"KurdishBLARK/InterdialectCorpus@{INTERDIALECT_REVISION}",
                              source_id=f"{relative}:{line_number}", license="CC-BY-NC-SA-4.0", commit=False)
                if (line_number + 1) % CHECKPOINT_INTERVAL == 0:
                    store.set_checkpoint(key, {"line": line_number + 1})
        store.set_checkpoint(key, {"line": last_line + 1})


def main() -> None:
    data_dir = Path(os.environ.get("DATA_DIR", "data")).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    action = sys.argv[1] if len(sys.argv) > 1 else "import"
    if action == "migrate-store":
        migrate_store(data_dir)
        return
    with Store(data_dir) as store:
        if action == "export":
            print(store.export())
            return
        if action != "import":
            raise SystemExit("Usage: python importer.py [import|export|migrate-store]")
        import_hf(store, data_dir)
        import_pewan(store, data_dir)
        import_bianet(store, data_dir)
        import_interdialect(store, data_dir)
        from wiki_import import import_wiki
        import_wiki(store, data_dir)
        print(f"Exported {store.export()}")


if __name__ == "__main__":
    main()
