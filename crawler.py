"""Polite, resumable Kurmanji article crawler.

Run with python crawler.py. Set DATA_DIR to choose where the durable SQLite
queue is stored. Article text is passed to Store(data_dir).add(...).

If DATA_DIR/kmr_zazaki_ngram.json exists, only high-confidence kmr_Latn model
results are accepted on mixed-language sources. A strong local heuristic may
also retain unknown model results inside explicitly trusted Kurmanji sections.
Confident Zazaki results are always rejected.
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
import subprocess
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import xml.etree.ElementTree as ET
import zlib
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
MAX_SITEMAP_BYTES = 64 * 1024 * 1024
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


def rojname_article_text(markup):
    from lxml import html as lxml_html

    page = lxml_html.fromstring(markup)
    bodies = page.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' post-body ') and starts-with(@id, 'post-body-')]")
    if len(bodies) != 1:
        return ""
    body = bodies[0]
    for element in body.xpath(".//script|.//style|.//nav|.//aside|.//iframe|.//form"):
        element.drop_tree()
    for element in body.xpath(".//br|.//p|.//div|.//li"):
        element.tail = "\n" + (element.tail or "")
    return "\n".join(line.strip() for line in body.text_content().splitlines() if line.strip())


class PostTextParser(HTMLParser):
    """Small HTML fallback for ANHA's div.post-text paragraphs."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.post_level = None
        self.p_depth = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag.lower() == "div" and "post-text" in (attrs.get("class") or "").split():
            self.post_level = len(self.stack)
        if tag.lower() == "p" and self.post_level is not None:
            self.p_depth += 1
            self.parts.append("\n")
        if tag.lower() not in {"area", "base", "br", "col", "embed", "hr", "img", "input",
                               "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append(tag.lower())

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "p" and self.p_depth:
            self.p_depth -= 1
            self.parts.append("\n")
        if tag == "div" and self.post_level is not None and len(self.stack) - 1 == self.post_level:
            self.post_level = None
            self.p_depth = 0
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i] == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        if self.post_level is not None and self.p_depth and data.strip():
            self.parts.append(data.strip())

    def text(self):
        return re.sub(r"\s+", " ", " ".join(self.parts)).strip()


def load_sources():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)["sources"]


def host_key(url):
    return (urllib.parse.urlsplit(url).hostname or "").lower()


def ascii_iri(url):
    parts = urllib.parse.urlsplit(url)
    path = urllib.parse.quote(parts.path, safe="/%:@!$&'()*+,;=-._~")
    query = urllib.parse.quote(parts.query, safe="/?%:@!$&'()*+,;=-._~&=")
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


class Crawler:
    def __init__(self, data_dir, sources):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.data_dir / "crawler.sqlite3"), timeout=30)
        self._closed = False
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.sources = {s["id"]: s for s in sources}
        self.last_request = {}
        self.robots = {}
        self.content_types = {}
        self.kmr_model = None
        self.kmr_model_error = None
        model_path = self.data_dir / "kmr_zazaki_ngram.json"
        self.kmr_model_configured = model_path.is_file()
        if model_path.is_file():
            try:
                from langid_kurdish import load_model
                self.kmr_model = load_model(model_path)
            except Exception as exc:
                self.kmr_model_error = str(exc)
                print(f"language model unavailable; new web records will be skipped: {exc}", file=sys.stderr)
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
          CREATE INDEX IF NOT EXISTS page_source_ready ON pages(source,state,discovered_at);
          CREATE INDEX IF NOT EXISTS page_ready_source ON pages(state,source,discovered_at);
          CREATE INDEX IF NOT EXISTS sitemap_source_state ON sitemaps(source,state,next_attempt);
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
        if source == "azadiya_welat" and path.rstrip("/") in {
            "", "/cihan", "/jin", "/niviskar", "/hevpeyvin", "/forum"
        }:
            return False
        if source == "lalishduhok" and urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).get("lang") != ["ku"]:
            return False
        pattern = self.sources[source].get("article_regex")
        return not pattern or re.search(pattern, path, re.I) is not None

    def _queue_page(self, url, source, now=None):
        url = ascii_iri(url)
        if source == "channel8" and host_key(url) == "channel8.com":
            url = url.replace("://channel8.com/", "://www.channel8.com/", 1)
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
                    final_url, payload = self._get(url, max_bytes=16 * 1024 * 1024)
                    if not self._allowed_source(final_url, source_id):
                        raise ValueError("homepage redirected outside configured source domain")
                    parser = LinkParser()
                    parser.feed(self._decode_html(payload, final_url))
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
                    final_url, payload = self._get(url, max_bytes=16 * 1024 * 1024)
                    if not self._allowed_source(final_url, source_id):
                        raise ValueError("feed redirected outside configured source domain")
                    now = time.time()
                    raw = self._decode_html(payload, final_url)
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
            _, payload = self._get(robots_url, max_bytes=1024 * 1024)
            lines = payload[:512 * 1024].decode("utf-8", "replace").splitlines()
            # RobotFileParser can merge adjacent user-agent groups when an
            # empty Disallow is removed. Convert it to an explicit allow-all
            # rule so subsequent bot-specific restrictions stay scoped.
            lines = ["Allow: /" if re.match(r"^\s*Disallow\s*:\s*$", line, re.I) else line
                     for line in lines]
            rp.parse(lines)
        except DeferredRequest:
            raise
        except urllib.error.HTTPError as e:
            if e.code in (401, 403) or e.code >= 500:
                rp.parse(["User-agent: *", "Disallow: /"])
            else:
                rp.parse([])
        except Exception:
            rp.parse(["User-agent: *", "Disallow: /"])
        self.robots[host] = rp
        return rp

    def _get(self, url, max_bytes=None):
        url = ascii_iri(url)
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
            "Accept-Encoding": "gzip, deflate, zstd",
        })
        if host == "www.kurdistantv.net":
            req.add_header("Accept", "*/*")
            req.add_header("Accept-Encoding", "gzip, deflate")
        self.last_request[host] = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                if max_bytes is None:
                    raw = response.read()
                else:
                    raw = response.read(max_bytes + 1)
                    if len(raw) > max_bytes:
                        raise ValueError(f"response exceeds {max_bytes} byte limit")
                final_url = ascii_iri(response.geturl())
                self.content_types[final_url] = response.headers.get("Content-Type", "")
                body = self._decode_content(raw, response.headers.get("Content-Encoding", ""), max_bytes)
                return final_url, body
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
    def _decode_content(payload, content_encoding, max_output):
        limit = max_output if max_output is not None else 32 * 1024 * 1024
        for encoding in reversed([x.strip().lower() for x in content_encoding.split(",") if x.strip()]):
            if encoding in {"gzip", "x-gzip"}:
                with gzip.GzipFile(fileobj=io.BytesIO(payload)) as zipped:
                    payload = zipped.read(limit + 1)
            elif encoding == "deflate":
                decompressor = zlib.decompressobj()
                payload = decompressor.decompress(payload, limit + 1)
                if decompressor.unconsumed_tail:
                    raise ValueError("deflate response exceeds decoded size limit")
            elif encoding == "zstd":
                import zstandard
                payload = zstandard.ZstdDecompressor().decompress(payload, max_output_size=limit + 1)
            else:
                continue
            if len(payload) > limit:
                raise ValueError("decoded response exceeds size limit")
        return payload

    def _decode_html(self, payload, url):
        content_type = self.content_types.get(url, "")
        match = re.search(r"charset\s*=\s*[\"']?([\w.-]+)", content_type, re.I)
        if not match:
            head = payload[:8192].decode("ascii", "ignore")
            match = re.search(r"<meta[^>]+charset=[\"']?([\w.-]+)", head, re.I)
        encoding = match.group(1) if match else "utf-8"
        try:
            decoded = payload.decode(encoding, "replace")
        except LookupError:
            decoded = payload.decode("utf-8", "replace")
        if decoded.count("\ufffd") > max(2, len(decoded) * 0.005):
            try:
                from charset_normalizer import from_bytes
                detected = from_bytes(payload).best()
                if detected and detected.encoding:
                    alternate = payload.decode(detected.encoding, "replace")
                    if alternate.count("\ufffd") < decoded.count("\ufffd"):
                        decoded = alternate
            except Exception:
                pass
        return decoded

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
            f"UPDATE {table} SET state='retry',attempts=?,next_attempt=?,last_error=?,updated_at=? WHERE url=?",
            (attempts, time.time() + error.delay, str(error)[:1000], time.time(), url))
        self.db.commit()

    def _record_error(self, table, url, error):
        row = self.db.execute(f"SELECT attempts FROM {table} WHERE url=?", (url,)).fetchone()
        attempts = (row[0] if row else 0) + 1
        delay = min(86400, 60 * (2 ** min(attempts - 1, 10)))
        state = "failed" if attempts >= MAX_ATTEMPTS else "retry"
        self.db.execute(f"UPDATE {table} SET state=?,attempts=?,next_attempt=?,last_error=?,updated_at=? WHERE url=?",
                        (state, attempts, time.time() + delay, str(error)[:1000], time.time(), url))
        self.db.commit()

    @staticmethod
    def _is_sitemap_index(root):
        return root.tag.rsplit("}", 1)[-1].lower() == "sitemapindex"

    def process_sitemaps(self, limit=MAX_SITEMAPS_PER_RUN):
        now = time.time()
        self.db.execute("UPDATE sitemaps SET state='retry' WHERE state='queued' AND next_attempt>?", (now,))
        self.db.execute("UPDATE sitemaps SET state='queued' WHERE state='retry' AND next_attempt<=?", (now,))
        self.db.execute(
            "UPDATE sitemaps SET state='queued',updated_at=? WHERE state='working' AND updated_at<?",
            (now, now - LEASE_SECONDS))
        self.db.commit()
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
                final_url, payload = self._get(url, max_bytes=MAX_SITEMAP_BYTES)
                if payload[:2] == b"\x1f\x8b":
                    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as zipped:
                        payload = zipped.read(MAX_SITEMAP_BYTES + 1)
                    if len(payload) > MAX_SITEMAP_BYTES:
                        raise ValueError("decompressed sitemap exceeds 64 MiB limit")
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

    def classify_kurmanji_text(self, text, source=None):
        """Return the optional high-confidence kmr/Zazaki model result."""
        if self.kmr_model is None:
            return {"label": "unknown", "score": None,
                    "reason": self.kmr_model_error or "model_unavailable"}
        from langid_kurdish import classify
        result = classify(text, self.kmr_model)
        if (source == "rojname_tr" and result["label"] == "unknown"
                and (result.get("score") or 0) > 0):
            model_path = self.data_dir / "lid.176.ftz"
            if model_path.is_file():
                try:
                    prediction = subprocess.run(
                        ["fasttext", "predict-prob", str(model_path), "-"],
                        input=" ".join(text.split()) + "\n", text=True, encoding="utf-8",
                        capture_output=True, check=True, timeout=5,
                    ).stdout.split()
                    if len(prediction) == 2 and prediction[0] == "__label__ku" and float(prediction[1]) >= 0.85:
                        return {**result, "label": "kmr_Latn", "reason": "positive_kmr_score_and_fasttext_ku"}
                except (OSError, ValueError, subprocess.SubprocessError):
                    pass
        return result

    def _trusted_kmr_section(self, url, source):
        config = self.sources[source]
        trusted_host = config.get("trusted_language_host")
        if trusted_host and host_key(url) != trusted_host.lower():
            return False
        trusted_path = config.get("trusted_language_path")
        path = urllib.parse.urlsplit(url).path.lower()
        return bool(trusted_path and path.startswith(trusted_path.lower()))

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
        article_type = bool(re.search(
            r"<meta\b(?=[^>]*property=[\"']og:type[\"'])(?=[^>]*content=[\"']article[\"'])[^>]*>",
            html, re.I))
        published = bool(meta and getattr(meta, "date", None)) or bool(re.search(
            r"(?:article:published_time|datePublished|<meta[^>]+name=[\"']date[\"'])",
            html, re.I))
        if len(segments) < 2 and not article_type and not published:
            return False
        if source == "azadiya_welat" and segments:
            if "-" not in segments[-1] and not article_type and not published:
                return False
        generic = {"home", "homepage", "latest", "search", "category", "categories", "archive", "tag", "tags"}
        if title.strip().lower() in generic:
            return False
        return len(text.strip()) >= 160

    def quarantine_stored_bad_routes(self, store):
        """Move only configured, verified bad web routes out of corpus storage."""
        count = 0
        sql_candidates = {
            "rudaw": "url LIKE '%/kurmanci/categories/multimedia' OR url LIKE '%/kurmanci/categories/multimedia/'",
            "jinha": "url LIKE '%/ku/%' AND instr(trim(substr(url,instr(url,'/ku/')+4),'/'),'/')=0",
            "azadiya_welat": "(" + " OR ".join(
                f"url LIKE '%azadiyawelat1.com{path}'"
                for path in ("/", "/cihan", "/cihan/", "/jin", "/jin/", "/niviskar",
                             "/niviskar/", "/hevpeyvin", "/hevpeyvin/", "/forum", "/forum/")
            ) + ")",
        }
        for source, sql_filter in sql_candidates.items():
            if source not in self.sources or not self.sources[source].get("quarantine_url_regex"):
                continue
            url_pattern = re.compile(self.sources[source]["quarantine_url_regex"], re.I)
            upper_row = store.conn.execute(
                f"SELECT MAX(id) FROM documents WHERE source=? AND ({sql_filter})", (source,)).fetchone()
            upper_id = upper_row[0]
            if upper_id is None:
                continue
            last_id = 0
            while True:
                batch = store.conn.execute(
                    "SELECT id,digest,text,url,source,title,date,upstream_source,source_id,license "
                    f"FROM documents WHERE source=? AND ({sql_filter}) AND id>? AND id<=? "
                    "ORDER BY id LIMIT 20", (source, last_id, upper_id)).fetchall()
                if not batch:
                    break
                last_id = batch[-1][0]
                selected = []
                for row in batch:
                    path = urllib.parse.urlsplit(row[3] or "").path
                    if url_pattern.fullmatch(path):
                        selected.append(row)
                if not selected:
                    continue
                now = time.time()
                self.db.executemany(
                    "INSERT OR REPLACE INTO quarantine_documents"
                    "(digest,text,url,source,title,date,upstream_source,source_id,license,quarantined_at,reason)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    [(*tuple(row[1:]), now, f"Verified bad route for {source}") for row in selected])
                self.db.commit()
                store.conn.executemany("DELETE FROM documents WHERE id=?", [(row[0],) for row in selected])
                store.commit()
                count += len(selected)
        return count

    def quarantine_stored_category_urls(self, store):
        """Backward-compatible name for the configured web-route quarantine."""
        return self.quarantine_stored_bad_routes(store)

    def classify_stored_web_documents(self, store, quarantine=False):
        """Classify only documents whose source is one of this crawler's web IDs.

        With quarantine=True, non-kmr records are moved into crawler.sqlite3 so
        their text and metadata remain recoverable. Other curated/HF sources are
        excluded by the source-ID filter.
        """
        if self.kmr_model is None:
            raise RuntimeError("kmr_zazaki_ngram.json is not available or could not be loaded")
        source_ids = tuple(self.sources)
        placeholders = ",".join("?" for _ in source_ids)
        counts = {"kmr_Latn": 0, "zazaki": 0, "unknown": 0, "quarantined": 0}
        max_row = store.conn.execute(
            f"SELECT MAX(id) FROM documents WHERE source IN ({placeholders})", source_ids).fetchone()
        upper_id = max_row[0]
        if upper_id is None:
            return counts
        last_id = 0
        query = (
            "SELECT id,digest,text,url,source,title,date,upstream_source,source_id,license "
            f"FROM documents WHERE source IN ({placeholders}) AND id>? AND id<=? ORDER BY id LIMIT 20"
        )
        while True:
            rows = store.conn.execute(query, (*source_ids, last_id, upper_id)).fetchall()
            if not rows:
                break
            last_id = rows[-1][0]
            rejected = []
            for row in rows:
                text = row[2] if isinstance(row[2], str) else zlib.decompress(row[2]).decode("utf-8")
                result = self.classify_kurmanji_text(text, row[4])
                label = result.get("label", "unknown")
                if label not in ("kmr_Latn", "zazaki"):
                    label = "unknown"
                trusted_unknown = (
                    label == "unknown"
                    and self._trusted_kmr_section(row[3], row[4])
                    and self._looks_kurmanji(text, row[3], row[4])
                )
                counts[label] += 1
                if trusted_unknown:
                    counts["unknown_trusted_section"] = counts.get("unknown_trusted_section", 0) + 1
                if quarantine and (label == "zazaki" or (label == "unknown" and not trusted_unknown)):
                    rejected.append(row)
            if rejected:
                now = time.time()
                reason = "retroactive language model result was not confident kmr_Latn"
                self.db.executemany(
                    "INSERT OR REPLACE INTO quarantine_documents"
                    "(digest,text,url,source,title,date,upstream_source,source_id,license,quarantined_at,reason)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    [(*tuple(row[1:]), now, reason) for row in rejected])
                self.db.commit()
                store.conn.executemany("DELETE FROM documents WHERE id=?", [(row[0],) for row in rejected])
                store.commit()
                counts["quarantined"] += len(rejected)
        return counts

    def close(self):
        if not self._closed:
            self.db.close()
            self._closed = True

    def process_pages(self, store):
        now = time.time()
        self.db.execute("UPDATE pages SET state='retry' WHERE state='queued' AND next_attempt>?", (now,))
        self.db.execute("UPDATE pages SET state='queued',lease_until=0 WHERE state='retry' AND next_attempt<=?", (now,))
        self.db.execute("UPDATE pages SET state='queued',lease_until=0 WHERE state='working' AND lease_until<?",
                        (now,))
        self.db.commit()
        refresh_retry_at = now
        while True:
            now = time.time()
            if now >= refresh_retry_at:
                self.db.execute("UPDATE pages SET state='queued' WHERE state='retry' AND next_attempt<=?",
                                (now,))
                self.db.commit()
                refresh_retry_at = now + 30
            row = self.db.execute(
                "SELECT url,source FROM pages INDEXED BY page_ready_source "
                "WHERE state='queued' AND source>? ORDER BY source,discovered_at LIMIT 1",
                (getattr(self, "_last_source", ""),)).fetchone()
            if not row:
                row = self.db.execute(
                    "SELECT url,source FROM pages INDEXED BY page_ready_source "
                    "WHERE state='queued' ORDER BY source,discovered_at LIMIT 1").fetchone()
            if not row:
                break
            now = time.time()
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
                final_url, payload = self._get(url, max_bytes=16 * 1024 * 1024)
                if not self._allowed_page(final_url, source):
                    raise ValueError("article redirected outside configured source domain")
                html = self._decode_html(payload, final_url)
                if source == "rojname_tr":
                    text = rojname_article_text(html)
                else:
                    text = trafilatura.extract(html, url=final_url, include_comments=False,
                                               include_tables=False, favor_precision=True)
                if source == "anha" and (not text or len(text.strip()) < 160):
                    post_text = PostTextParser()
                    post_text.feed(html)
                    fallback = post_text.text()
                    if fallback:
                        text = fallback
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
                if self.kmr_model_configured:
                    result = self.classify_kurmanji_text(text, source)
                    label = result.get("label", "unknown")
                    trusted_unknown = (
                        label == "unknown"
                        and self.kmr_model is not None
                        and self._trusted_kmr_section(final_url, source)
                        and self._looks_kurmanji(text, final_url, source)
                    )
                    if label == "zazaki" or (label != "kmr_Latn" and not trusted_unknown):
                        reason = f"language model {result.get('label', 'unknown')}: {result.get('reason', 'no confident kmr result')}"
                        self.db.execute("UPDATE pages SET state='skipped',last_error=?,updated_at=? WHERE url=?",
                                        (reason[:1000], time.time(), url))
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
            self.close()


def main():
    parser = argparse.ArgumentParser(description="Crawl configured Kurmanji web sources")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--quarantine-stored-category-urls", action="store_true",
                         help="move only configured, verified web menu routes to quarantine")
    actions.add_argument("--classify-stored-web-documents", action="store_true",
                         help="classify stored documents from crawler source IDs only")
    actions.add_argument("--quarantine-unclassified-web-documents", action="store_true",
                         help="quarantine crawler web documents not confidently classified as Kurmanji")
    args = parser.parse_args()
    data_dir = os.environ.get("DATA_DIR", str(HERE / "data"))
    sources = load_sources()
    from store import Store
    with Store(data_dir) as store:
        crawler = Crawler(data_dir, sources)
        try:
            if args.quarantine_stored_category_urls:
                count = crawler.quarantine_stored_bad_routes(store)
                print(f"quarantined {count} verified web-route records; preserved in crawler.sqlite3")
            elif args.classify_stored_web_documents or args.quarantine_unclassified_web_documents:
                summary = crawler.classify_stored_web_documents(
                    store, quarantine=args.quarantine_unclassified_web_documents)
                print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
            else:
                crawler.run(store)
        finally:
            crawler.close()


if __name__ == "__main__":
    main()
