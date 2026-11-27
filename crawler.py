"""Polite, resumable Kurmanji article crawler.

Run with python crawler.py. Set DATA_DIR to choose where the durable SQLite
queue is stored. Article text is passed to Store(data_dir).add(...).
"""

from __future__ import annotations

import json
import gzip
import io
import email.utils
import datetime
import argparse
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path

import trafilatura

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "crawler_sources.json"
USER_AGENT = "KurdishCorpusResearchBot/1.0 (+https://github.com/rizgan/kurdish-corpus)"
REQUEST_TIMEOUT = 25
HOST_DELAY_SECONDS = 2.0
MAX_ATTEMPTS = 5
LEASE_SECONDS = 300
MAX_SITEMAPS_PER_RUN = 24
MAX_SITEMAP_BYTES = 128 * 1024 * 1024
KM_RUNE = re.compile(r"[çÇêÊîÎşŞûÛ]")
KM_DISTINCTIVE_RUNE = re.compile(r"[êîûÊÎÛ]")
KM_WORDS = set("ez tu ew em hûn wan ji bi di û ku ne ye heye dike kirin got rojê gelê kurdî kurdistanê kurmancî dema piştî berî ji bo li ser êdî çima çawa kesên dibêje dibêjin lê her wê niha xwe civak heta pêş".split())
TR_WORDS = set("bir bu ve ile için olarak olan daha ancak fakat çünkü tarafından yapılan yapıldı edildi dedi söyledi açıklama sonra önce arasında bugün".split())
ARABIC_SCRIPT = re.compile(r"[\u0600-\u06ff\u0750-\u077f]")
NON_ARTICLE_SEGMENTS = {
    "category", "categories", "kategori", "tag", "tags", "author", "authors",
    "search", "page", "page-1", "archive", "archives", "arsiv", "rss", "feed",
    "api", "wp-json", "login", "register", "contact", "about", "privacy",
    "multimedia", "video", "videos", "photo", "photos",
}


class DeferredRequest(Exception):
    def __init__(self, delay, message="request deferred by host backoff"):
        super().__init__(message)
        self.delay = max(1.0, float(delay))


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)


def load_sources():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)["sources"]


def host_key(url):
    return (urllib.parse.urlsplit(url).hostname or "").lower()


class Crawler:
    def __init__(self, data_dir, sources):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.data_dir / "crawler.sqlite3"), timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.sources = {s["id"]: s for s in sources}
        self.last_request = {}
        self.robots = {}
        self._schema()
        self._prepare_sitemaps()

    def _schema(self):
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS sitemaps(
            url TEXT PRIMARY KEY, source TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            last_error TEXT, updated_at REAL NOT NULL
          );
          CREATE TABLE IF NOT EXISTS pages(
            url TEXT PRIMARY KEY, source TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            lease_until REAL NOT NULL DEFAULT 0, last_error TEXT, discovered_at REAL NOT NULL,
            updated_at REAL NOT NULL
          );
          CREATE INDEX IF NOT EXISTS page_work ON pages(state,next_attempt,lease_until);
          CREATE TABLE IF NOT EXISTS host_backoff(
            host TEXT PRIMARY KEY, until_at REAL NOT NULL, reason TEXT NOT NULL DEFAULT ''
          );
          CREATE TABLE IF NOT EXISTS quarantine_documents(
            digest BLOB PRIMARY KEY, text BLOB NOT NULL, url TEXT NOT NULL,
            source TEXT NOT NULL, title TEXT NOT NULL, date TEXT NOT NULL,
            upstream_source TEXT NOT NULL, source_id TEXT NOT NULL, license TEXT NOT NULL,
            quarantined_at REAL NOT NULL, reason TEXT NOT NULL
          ) WITHOUT ROWID;
        """)
        self.db.commit()

    def _prepare_sitemaps(self):
        now = time.time()
        for source in self.sources.values():
            for url in source.get("sitemaps", []):
                self.db.execute("INSERT OR IGNORE INTO sitemaps(url,source,updated_at) VALUES(?,?,?)",
                                (url, source["id"], now))
        self.db.commit()

    def _allowed_source(self, url, expected_source):
        p = urllib.parse.urlsplit(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").lower()
        return any(host == d.lower() or host.endswith("." + d.lower())
                   for d in self.sources[expected_source]["domains"])

    def _allowed_page(self, url, source):
        if not self._allowed_source(url, source):
            return False
        path = urllib.parse.urlsplit(url).path.lower()
        segments = [part for part in path.split("/") if part]
        rudaw_numeric_article = (
            source == "rudaw" and len(segments) >= 3
            and segments[0] == "kurmanci" and segments[-1].isdigit()
        )
        if any(part in NON_ARTICLE_SEGMENTS for part in segments) and not rudaw_numeric_article:
            return False
        if path.lower().endswith((".xml", ".xml.gz", ".rss", ".json", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf")):
            return False
        routes = self.sources[source].get("article_paths", [])
        if routes and not any(path.startswith(route.lower()) for route in routes):
            return False
        pattern = self.sources[source].get("article_regex")
        return not pattern or re.search(pattern, path, re.I) is not None

    def _queue_page(self, url, source, now=None):
        if not self._allowed_page(url, source):
            return
        url, _ = urllib.parse.urldefrag(url)
        if not url:
            return
        now = now or time.time()
        self.db.execute("INSERT OR IGNORE INTO pages(url,source,discovered_at,updated_at) VALUES(?,?,?,?)",
                        (url, source, now, now))

    def process_homepages(self):
        """Refresh configured language-section pages on every scheduled run."""
        for source in self.sources.values():
            source_id = source["id"]
            for url in source.get("homepages", []):
                try:
                    if not self._allowed_source(url, source_id):
                        raise ValueError("homepage host is outside configured source domains")
                    if not self._get_robots(url).can_fetch(USER_AGENT, url):
                        print(f"homepage disallowed by robots.txt: {url}", file=sys.stderr)
                        continue
                    final_url, payload = self._get(url)
                    if not self._allowed_source(final_url, source_id):
                        raise ValueError("homepage redirected outside configured source domain")
                    parser = LinkParser()
                    parser.feed(payload.decode("utf-8", "replace"))
                    now = time.time()
                    for href in parser.links:
                        link = urllib.parse.urljoin(final_url, href)
                        self._queue_page(link, source_id, now)
                    self.db.commit()
                    print(f"homepage refreshed {source_id}: {final_url}")
                except Exception as e:
                    print(f"homepage error {url}: {e}", file=sys.stderr)
            for url in source.get("feeds", []):
                try:
                    if not self._allowed_source(url, source_id):
                        raise ValueError("feed host is outside configured source domains")
                    if not self._get_robots(url).can_fetch(USER_AGENT, url):
                        print(f"feed disallowed by robots.txt: {url}", file=sys.stderr)
                        continue
                    final_url, payload = self._get(url)
                    if not self._allowed_source(final_url, source_id):
                        raise ValueError("feed redirected outside configured source domain")
                    now = time.time()
                    raw = payload.decode("utf-8", "replace")
                    try:
                        candidates = []
                        if raw.lstrip().startswith("<"):
                            root = ET.fromstring(raw)
                            for node in root.iter():
                                local = node.tag.rsplit("}", 1)[-1].lower()
                                if local in {"link", "loc"}:
                                    href = node.attrib.get("href")
                                    if href:
                                        candidates.append(href)
                                    if node.text and node.text.strip():
                                        candidates.append(node.text.strip())
                        else:
                            data = json.loads(raw)
                            def collect(value):
                                if isinstance(value, dict):
                                    for k, v in value.items():
                                        if isinstance(v, str) and k.lower() in {"url", "link", "href", "permalink", "canonicalurl"}:
                                            candidates.append(v)
                                        elif isinstance(v, str) and "href" in v.lower():
                                            links = LinkParser()
                                            links.feed(v)
                                            candidates.extend(links.links)
                                        else:
                                            collect(v)
                                elif isinstance(value, list):
                                    for item in value:
                                        collect(item)
                            collect(data)
                    except (json.JSONDecodeError, ET.ParseError):
                        candidates = re.findall(r"""(?:href|url|link|permalink)["'=:\s]+["']([^"' ]+)""", raw, re.I)
                    for candidate in candidates:
                        self._queue_page(urllib.parse.urljoin(final_url, candidate), source_id, now)
                    self.db.commit()
                    print(f"feed refreshed {source_id}: {final_url}")
                except Exception as e:
                    print(f"feed error {url}: {e}", file=sys.stderr)

    def _get_robots(self, url):
        host = host_key(url)
        if host in self.robots:
            return self.robots[host]
        rp = urllib.robotparser.RobotFileParser()
        robots_url = urllib.parse.urlunsplit((urllib.parse.urlsplit(url).scheme, host, "/robots.txt", "", ""))
        rp.set_url(robots_url)
        try:
            _, payload = self._get(robots_url)
            rp.parse(payload[:512 * 1024].decode("utf-8", "replace").splitlines())
        except urllib.error.HTTPError as e:
            if e.code in (401, 403) or e.code >= 500:
                rp.parse(["User-agent: *", "Disallow: /"])
            else:
                rp.parse([])
        except Exception:
            rp.parse(["User-agent: *", "Disallow: /"])
        self.robots[host] = rp
        return rp

    def _get(self, url):
        host = host_key(url)
        row = self.db.execute("SELECT until_at FROM host_backoff WHERE host=?", (host,)).fetchone()
        if row and row["until_at"] > time.time():
            raise DeferredRequest(row["until_at"] - time.time())
        wait = HOST_DELAY_SECONDS - (time.monotonic() - self.last_request.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml,text/xml;q=0.9,*/*;q=0.5",
        })
        self.last_request[host] = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                return response.geturl(), response.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                retry_after = self._retry_after_seconds(e.headers.get("Retry-After"))
                delay = min(86400.0, retry_after if retry_after is not None else 60.0)
                until_at = time.time() + delay
                self.db.execute(
                    "INSERT INTO host_backoff(host,until_at,reason) VALUES(?,?,?) "
                    "ON CONFLICT(host) DO UPDATE SET until_at=MAX(until_at,excluded.until_at),reason=excluded.reason",
                    (host, until_at, f"HTTP {e.code}"))
                self.db.commit()
                raise DeferredRequest(delay, f"HTTP {e.code}; host deferred for {delay:.0f}s") from e
            raise

    @staticmethod
    def _retry_after_seconds(value):
        if not value:
            return None
        try:
            return max(0.0, float(value.strip()))
        except (ValueError, AttributeError):
            try:
                dt = email.utils.parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return max(0.0, (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    def _record_deferred(self, table, url, error):
        row = self.db.execute("SELECT attempts FROM " + table + " WHERE url=?", (url,)).fetchone()
        attempts = row[0] if row else 0
        self.db.execute(
            f"UPDATE {table} SET state='queued',attempts=?,next_attempt=?,last_error=?,updated_at=? WHERE url=?",
            (attempts, time.time() + error.delay, str(error)[:1000], time.time(), url))
        self.db.commit()

    def _record_error(self, table, url, error):
        row = self.db.execute(f"SELECT attempts FROM {table} WHERE url=?", (url,)).fetchone()
        attempts = (row[0] if row else 0) + 1
        delay = min(86400, 60 * (2 ** min(attempts - 1, 10)))
        state = "failed" if attempts >= MAX_ATTEMPTS else "queued"
        self.db.execute(f"UPDATE {table} SET state=?,attempts=?,next_attempt=?,last_error=?,updated_at=? WHERE url=?",
                        (state, attempts, time.time() + delay, str(error)[:1000], time.time(), url))
        self.db.commit()

    @staticmethod
    def _is_sitemap_index(root):
        return root.tag.rsplit("}", 1)[-1].lower() == "sitemapindex"

    def process_sitemaps(self, limit=MAX_SITEMAPS_PER_RUN):
        processed = 0
        while processed < limit:
            row = self.db.execute("SELECT url,source FROM sitemaps WHERE state='queued' AND next_attempt<=? ORDER BY url LIMIT 1",
                                  (time.time(),)).fetchone()
            if not row:
                break
            url, source = row["url"], row["source"]
            self.db.execute("UPDATE sitemaps SET state='working',updated_at=? WHERE url=?", (time.time(), url))
            self.db.commit()
            try:
                if not self._allowed_source(url, source):
                    raise ValueError("sitemap host is outside configured source domains")
                if not self._get_robots(url).can_fetch(USER_AGENT, url):
                    raise PermissionError("robots.txt disallows sitemap")
                final_url, payload = self._get(url)
                if payload[:2] == b"\x1f\x8b":
                    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as zipped:
                        payload = zipped.read(MAX_SITEMAP_BYTES + 1)
                    if len(payload) > MAX_SITEMAP_BYTES:
                        raise ValueError("decompressed sitemap exceeds 128 MiB limit")
                root = ET.fromstring(payload)
                now = time.time()
                for loc in root.findall(".//{*}loc"):
                    link = (loc.text or "").strip()
                    if not link:
                        continue
                    if self._is_sitemap_index(root):
                        if not self._allowed_source(link, source):
                            continue
                        self.db.execute("INSERT OR IGNORE INTO sitemaps(url,source,updated_at) VALUES(?,?,?)",
                                        (link, source, now))
                    else:
                        self._queue_page(link, source, now)
                self.db.execute("UPDATE sitemaps SET state='done',attempts=attempts+1,last_error=NULL,updated_at=? WHERE url=?",
                                (now, url))
                self.db.commit()
                processed += 1
                print(f"sitemap {source}: {final_url}")
            except DeferredRequest as e:
                processed += 1
                self._record_deferred("sitemaps", url, e)
                print(f"sitemap deferred {url}: {e}", file=sys.stderr)
            except Exception as e:
                processed += 1
                self._record_error("sitemaps", url, e)
                print(f"sitemap error {url}: {e}", file=sys.stderr)

    def _looks_kurmanji(self, text, url, source):
        section = self.sources[source].get("language_path", "").lower()
        path = urllib.parse.urlsplit(url).path.lower()
        explicit = bool(section and section != "/" and section in path) or any(
            token in path for token in ("/ku/", "/kurmanci", "/kurmancî", "/kurdî/")
        )
        arabic = len(ARABIC_SCRIPT.findall(text))
        if arabic > max(30, len(text) * 0.08):
            return False
        words = re.findall(r"[a-zçêîşû]+", text.lower())
        if not words:
            return False
        word_counts = {}
        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1
        hits = sum(word_counts.get(word, 0) for word in KM_WORDS)
        strong_words = set("hûn piştî êdî çima çawa dibêje dibêjin xwe dibin dibe gotin herwiha civak dîrok".split())
        strong_hits = sum(word_counts.get(word, 0) for word in strong_words)
        turkish_hits = sum(word_counts.get(word, 0) for word in TR_WORDS)
        turkish_signals = sum(1 for word in TR_WORDS if word_counts.get(word, 0))
        if turkish_signals >= 3 and turkish_hits >= max(5, len(words) * 0.035) and strong_hits < 3:
            return False
        distinctive = len(KM_DISTINCTIVE_RUNE.findall(text))
        return (explicit and (hits >= 5 or distinctive >= 5 or strong_hits >= 3)) or (
            hits >= 8 and (distinctive >= 2 or strong_hits >= 3))

    @staticmethod
    def _is_article_document(url, html, text, meta, title, source):
        path = urllib.parse.urlsplit(url).path.lower()
        segments = [part for part in path.split("/") if part]
        rudaw_numeric_article = (
            source == "rudaw" and len(segments) >= 3
            and segments[0] == "kurmanci" and segments[-1].isdigit()
        )
        if any(part in NON_ARTICLE_SEGMENTS for part in segments) and not rudaw_numeric_article:
            return False
        article_type = bool(re.search(r"property=[\"']og:type[\"'][^>]*content=[\"']article", html, re.I))
        published = bool(re.search(
            r"(?:article:published_time|datePublished|<meta[^>]+name=[\"']date[\"'])",
            html, re.I))
        if len(segments) < 2 and not article_type and not published:
            return False
        generic = {"home", "homepage", "latest", "search", "category", "categories", "archive", "tag", "tags"}
        if title.strip().lower() in generic:
            return False
        return len(text.strip()) >= 160

    def quarantine_stored_category_urls(self, store):
        """Move known Rudaw category/menu records out of corpus storage, preserving them."""
        count = 0
        last_digest = b""
        while True:
            batch = store.conn.execute(
                "SELECT digest,text,url,source,title,date,upstream_source,source_id,license "
                "FROM documents WHERE source='rudaw' AND url LIKE '%/kurmanci/categories/%' "
                "AND digest>? ORDER BY digest LIMIT 100", (last_digest,)).fetchall()
            if not batch:
                break
            last_digest = batch[-1][0]
            selected = []
            for row in batch:
                path = urllib.parse.urlsplit(row[2] or "").path.rstrip("/")
                final_segment = path.rsplit("/", 1)[-1]
                if not final_segment.isdigit():
                    selected.append(row)
            if not selected:
                continue
            now = time.time()
            self.db.executemany(
                "INSERT OR REPLACE INTO quarantine_documents"
                "(digest,text,url,source,title,date,upstream_source,source_id,license,quarantined_at,reason)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                [(*tuple(row), now, "Rudaw category/menu URL with nonnumeric final path segment") for row in selected])
            self.db.commit()
            store.conn.executemany("DELETE FROM documents WHERE digest=?", [(row[0],) for row in selected])
            store.commit()
            count += len(selected)
        return count

    def process_pages(self, store):
        while True:
            now = time.time()
            row = self.db.execute(
                "SELECT url,source FROM pages "
                "WHERE ((state='queued' AND next_attempt<=?) OR (state='working' AND lease_until<?)) "
                "AND source>? ORDER BY source,discovered_at LIMIT 1",
                (now, now, getattr(self, "_last_source", ""))).fetchone()
            if not row:
                row = self.db.execute(
                    "SELECT url,source FROM pages "
                    "WHERE ((state='queued' AND next_attempt<=?) OR (state='working' AND lease_until<?)) "
                    "ORDER BY source,discovered_at LIMIT 1", (now, now)).fetchone()
            if not row:
                break
            url, source = row["url"], row["source"]
            self._last_source = source
            self.db.execute("UPDATE pages SET state='working',lease_until=?,updated_at=? WHERE url=?",
                            (now + LEASE_SECONDS, now, url))
            self.db.commit()
            try:
                if not self._allowed_page(url, source):
                    self.db.execute("UPDATE pages SET state='skipped',last_error='URL is outside configured article paths',updated_at=? WHERE url=?",
                                    (time.time(), url))
                    self.db.commit()
                    continue
                if not self._get_robots(url).can_fetch(USER_AGENT, url):
                    self.db.execute("UPDATE pages SET state='skipped',last_error='robots.txt disallows URL',updated_at=? WHERE url=?",
                                    (time.time(), url))
                    self.db.commit()
                    continue
                final_url, payload = self._get(url)
                if not self._allowed_page(final_url, source):
                    raise ValueError("article redirected outside configured source domain")
                html = payload.decode("utf-8", "replace")
                text = trafilatura.extract(html, url=final_url, include_comments=False,
                                           include_tables=False, favor_precision=True)
                meta = trafilatura.extract_metadata(html, default_url=final_url)
                title = (meta.title or "").strip() if meta else ""
                date = (meta.date or "").strip() if meta else ""
                if not text or len(text.strip()) < 160:
                    raise ValueError("article text too short after Trafilatura extraction")
                if not self._is_article_document(final_url, html, text, meta, title, source):
                    self.db.execute("UPDATE pages SET state='skipped',last_error='not selected as an article page',updated_at=? WHERE url=?",
                                    (time.time(), url))
                    self.db.commit()
                    continue
                if not self._looks_kurmanji(text, final_url, source):
                    self.db.execute("UPDATE pages SET state='skipped',last_error='not selected as kmr by local heuristic',updated_at=? WHERE url=?",
                                    (time.time(), url))
                    self.db.commit()
                    continue
                store.add(text.strip(), final_url, source, title=title, date=date)
                self.db.execute("UPDATE pages SET state='done',attempts=attempts+1,lease_until=0,last_error=NULL,updated_at=? WHERE url=?",
                                (time.time(), url))
                self.db.commit()
                print(f"stored {source}: {title or final_url}")
            except DeferredRequest as e:
                self._record_deferred("pages", url, e)
            except Exception as e:
                self._record_error("pages", url, e)
                self.db.execute("UPDATE pages SET lease_until=0 WHERE url=?", (url,))
                self.db.commit()
                print(f"article error {url}: {e}", file=sys.stderr)

    def run(self, store):
        try:
            self.process_homepages()
            self.process_sitemaps()
            count = self.db.execute("SELECT state,COUNT(*) n FROM pages GROUP BY state").fetchall()
            print("queue:", ", ".join(f"{r['state']}={r['n']}" for r in count) or "empty")
            self.process_pages(store)
        finally:
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description="Crawl configured Kurmanji web sources")
    parser.add_argument("--quarantine-stored-category-urls", action="store_true",
                        help="move already stored Rudaw category URLs to crawler quarantine")
    args = parser.parse_args()
    data_dir = os.environ.get("DATA_DIR", str(HERE / "data"))
    sources = load_sources()
    from store import Store
    with Store(data_dir) as store:
        crawler = Crawler(data_dir, sources)
        if args.quarantine_stored_category_urls:
            count = crawler.quarantine_stored_category_urls(store)
            print(f"quarantined {count} category records; preserved in crawler.sqlite3")
            crawler.db.close()
        else:
            crawler.run(store)


if __name__ == "__main__":
    main()
