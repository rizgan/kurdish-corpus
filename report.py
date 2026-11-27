"""Offline one-pass corpus, crawler queue, and disk usage report."""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import os
import re
import shutil
import sqlite3
import zlib
from collections import Counter
from pathlib import Path

WORD_RE = re.compile(r"[^\W_]+(?:['’ʼ-][^\W_]+)*", re.UNICODE)


def corpus_summary(data_dir: Path) -> dict:
    db_path = data_dir / "corpus.sqlite3"
    if not db_path.is_file():
        raise FileNotFoundError(f"Corpus database not found: {db_path}")
    uri = db_path.resolve().as_uri() + "?mode=ro"
    totals: dict[str, Counter] = {}
    with closing(sqlite3.connect(uri, uri=True)) as db:
        cursor = db.execute("SELECT source, text FROM documents")
        while batch := cursor.fetchmany(128):
            for source, packed in batch:
                source = source or "(без источника)"
                text = packed if isinstance(packed, str) else zlib.decompress(packed).decode("utf-8")
                count = totals.setdefault(source, Counter())
                count["documents"] += 1
                count["words"] += len(WORD_RE.findall(text))
    return {source: dict(count) for source, count in sorted(totals.items(), key=lambda item: item[0].casefold())}


def crawler_summary(data_dir: Path, sample_errors: int) -> dict:
    db_path = data_dir / "crawler.sqlite3"
    if not db_path.is_file():
        return {"available": False, "message": "crawler.sqlite3 отсутствует"}
    result = {"available": True, "tables": {}}
    with closing(sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in ("sitemaps", "pages"):
            if table not in existing:
                continue
            states = {state or "(пусто)": count for state, count in
                      db.execute(f"SELECT state, COUNT(*) FROM {table} GROUP BY state")}
            failed = []
            if sample_errors > 0:
                failed = [
                    {"url": url, "source": source, "error": error}
                    for url, source, error in db.execute(
                        f"SELECT url, source, last_error FROM {table} "
                        "WHERE state='failed' ORDER BY updated_at DESC LIMIT ?", (sample_errors,)
                    )
                ]
            result["tables"][table] = {"states": states, "failure_samples": failed}
    return result


def disk_summary(data_dir: Path) -> dict:
    usage = shutil.disk_usage(data_dir)
    stored_bytes = sum(path.stat().st_size for path in data_dir.rglob("*") if path.is_file())
    gib = 1024 ** 3
    return {
        "data_dir_bytes": stored_bytes,
        "filesystem_total_gib": round(usage.total / gib, 2),
        "filesystem_used_gib": round((usage.total - usage.free) / gib, 2),
        "filesystem_free_gib": round(usage.free / gib, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path,
                        default=Path(os.environ.get("DATA_DIR", "data")),
                        help="Каталог с corpus.sqlite3 и crawler.sqlite3 (или DATA_DIR)")
    parser.add_argument("--json", action="store_true", help="Вывести JSON")
    parser.add_argument("--sample-errors", type=int, default=5,
                        help="Сколько последних ошибок обходчика показать на таблицу; 0 отключает")
    args = parser.parse_args()
    data_dir = args.data_dir.expanduser().resolve()
    report = {
        "data_dir": str(data_dir),
        "corpus_by_source": corpus_summary(data_dir),
        "crawler": crawler_summary(data_dir, max(0, args.sample_errors)),
        "disk": disk_summary(data_dir),
    }
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(f"Каталог данных: {data_dir}")
    print("\nКорпус по источникам:")
    if report["corpus_by_source"]:
        print(f"{'Источник':30} {'Документов':>14} {'Слов':>16}")
        for source, counts in report["corpus_by_source"].items():
            print(f"{source[:30]:30} {counts['documents']:14,} {counts['words']:16,}")
    else:
        print("  пусто")
    print("\nОчередь обходчика:")
    crawler = report["crawler"]
    if not crawler["available"]:
        print(f"  {crawler['message']}")
    for table, summary in crawler.get("tables", {}).items():
        states = ", ".join(f"{state}={count:,}" for state, count in sorted(summary["states"].items())) or "пусто"
        print(f"  {table}: {states}")
        for failure in summary["failure_samples"]:
            print(f"    {failure['source']} | {failure['url']} | {failure['error']}")
    disk = report["disk"]
    print("\nДиск:")
    print(f"  файлы данных: {disk['data_dir_bytes'] / 1024**3:.2f} GiB; "
          f"свободно на разделе: {disk['filesystem_free_gib']:.2f} GiB "
          f"из {disk['filesystem_total_gib']:.2f} GiB")


if __name__ == "__main__":
    main()
