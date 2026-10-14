#!/usr/bin/env python3
"""Copy "unlimited" (photoslice) photos into a normal Yandex.Disk folder.

The hidden unlimited photo storage is not visible to the public REST API /
WebDAV / desktop sync. The Disk web app manages it through an internal BFF:

    POST https://disk.yandex.ru/models-v2?m=<method>

This tool uses the same endpoint with your browser cookies, but only performs
a *server-side copy* into a regular folder. Nothing is downloaded, nothing is
deleted, and the web-UI "max 500 selected" limit does not apply because the
copy happens on Yandex's side.

The destination folder is then picked up by Synology Cloud Sync.

Auth
----
Export cookies from a logged-in https://disk.yandex.ru session with the
"Cookie-Editor" browser extension (Export -> Export as JSON) and save them as
cookies.json next to this script.

Usage
-----
    python backup_photos.py --list
        Inventory only. Safe. Appends to .photoslice_paths.txt and is
        resumable. Use --refresh to start over.

    python backup_photos.py --probe --dest /PhotoBackup
        Copies ONE photo and prints the raw request + response.

    python backup_photos.py --copy --dest /PhotoBackup
        Pipeline: collects and copies at the same time. Gentle by default:
        one worker, a shared 5s pause between requests, and retries with
        exponential backoff (--workers/--copy-delay/--retries). Resumable;
        already-copied files are skipped.

    python backup_photos.py --retry-failed --dest /PhotoBackup
        Re-copy only the files that ended up in .failed.txt (usually because
        the account had too many async copies executing). Run it after the
        server-side backlog drains.

    python backup_photos.py --copy --dest /PhotoBackup --from-cache
        Skips collection, copies whatever is already in the cache (parallel).

Why "TooManyExecutingOperationsError"
-------------------------------------
Yandex caps how many *server-side* copies may be executing/queued per account
at once. Firing large batches faster than they drain fills that queue and every
subsequent operation fails with TooManyExecutingOperationsError. This tool
therefore throttles globally (min interval between requests), backs off hard on
that specific error, and records files that never got through in .failed.txt so
they can be retried later without re-scanning.

Scope
-----
By default only *unlimited* photos are copied. The "all photos" view mixes two
kinds, told apart by the album token:
  * unlimited uploads  -> album "photounlim",      path /photounlim/<name>
  * regular uploads    -> album "nonphotounlim",   path /disk/Фотокамера/<name>
Regular uploads already sit in a normal folder that Cloud Sync mirrors, so
they are skipped. Pass --all-photos to include them again.

Notes
-----
* Normal Disk folders are addressed as /disk/<name> in this API; the /disk
  prefix is added to --dest automatically.
* Copying is asynchronous: the file appears in the destination a few seconds
  after the call returns.
* Re-running is safe. The destination folder is listed via `mpfs/list` before
  copying and any file already there is skipped. On top of that, copied sources
  are remembered in .copied.txt, and Yandex itself never creates "name (1)"
  duplicates: copying onto an existing name returns 412 "resource already
  exists", which this script counts as skipped.

Nothing here deletes anything.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import random
import re
import sys
import threading
import time
from collections import Counter

try:  # prefer TLS-impersonation to look like a real browser
    from curl_cffi import requests  # type: ignore

    _SESSION_KW = {"impersonate": "firefox"}
except ImportError:  # pragma: no cover - fallback path
    import requests  # type: ignore

    _SESSION_KW = {}

BASE = "https://disk.yandex.ru"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

HERE = os.path.dirname(os.path.abspath(__file__))
COOKIES_FILE = os.path.join(HERE, "cookies.json")
PATHS_CACHE = os.path.join(HERE, ".photoslice_paths.txt")
COPIED_CACHE = os.path.join(HERE, ".copied.txt")
FAILED_CACHE = os.path.join(HERE, ".failed.txt")
COLLECT_STATE = os.path.join(HERE, ".collect_state.json")

COPY_METHOD = "mpfs/bulk-async-copy"


def log(msg: str) -> None:
    print(msg, flush=True)


def to_mpfs_path(p: str) -> str:
    """Photoslice returns 'photounlim:/name.JPG'; mpfs wants '/photounlim/name.JPG'."""
    if p.startswith("photounlim:/"):
        return "/photounlim/" + p[len("photounlim:/"):]
    if p.startswith("photounlim:"):
        return "/photounlim/" + p[len("photounlim:"):].lstrip("/")
    return p if p.startswith("/") else "/" + p


def is_unlimited(item: dict) -> bool:
    """Unlimited (photoslice) photos carry the exact album token 'photounlim'.

    Regular camera uploads carry 'nonphotounlim', which *contains* the same
    substring, so this must be an exact token match, not a substring test.
    Also note their path lives under /photounlim/ rather than /disk/.
    """
    return "photounlim" in (item.get("albums") or [])


def unlimited_paths_only(paths: list[str]) -> list[str]:
    """Keep only unlimited-photo paths (they live under /photounlim/)."""
    return [p for p in paths if p.startswith("/photounlim/")]


def load_cookies(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        sys.exit(
            f"Нет {path}. Экспортируй куки из браузера (Cookie-Editor -> "
            f"Export as JSON) и положи рядом со скриптом как cookies.json"
        )
    raw = json.loads(open(path, encoding="utf-8").read())
    if not isinstance(raw, list):
        sys.exit("cookies.json должен быть JSON-массивом (формат Cookie-Editor)")
    return {
        c["name"]: c["value"]
        for c in raw
        if c.get("name") and c.get("value") is not None
    }


def authorize(cookies: dict[str, str]) -> tuple[str, str]:
    s = requests.Session(**_SESSION_KW)
    s.headers.update({"User-Agent": UA, "Accept-Language": "ru,en;q=0.9"})
    for name, value in cookies.items():
        s.cookies.set(name, value, domain=".yandex.ru")
    r = s.get(f"{BASE}/client/disk", timeout=30)
    if r.status_code != 200 or "passport.yandex" in r.url:
        sys.exit("Куки невалидны или истекли — обнови cookies.json")
    sk = re.search(r'"sk"\s*:\s*"([^"]+)"', r.text)
    uid = re.search(r'"uid"\s*:\s*"?(\d+)"?', r.text)
    if not sk or not uid:
        sys.exit("Не нашёл sk/uid в HTML — Яндекс изменил формат страницы")
    return uid.group(1), sk.group(1)


class BFF:
    """Thread-safe client. Each thread gets its own HTTP session; the rotating
    CSRF token `sk` is shared under a lock."""

    def __init__(self, cookies: dict[str, str], uid: str, sk: str) -> None:
        self.cookies = cookies
        self.uid = uid
        self._sk = sk
        self._lock = threading.Lock()
        self._tls = threading.local()

    @property
    def sk(self) -> str:
        with self._lock:
            return self._sk

    def _set_sk(self, value: str) -> None:
        with self._lock:
            self._sk = value

    def _session(self):
        s = getattr(self._tls, "s", None)
        if s is None:
            s = requests.Session(**_SESSION_KW)
            s.headers.update(
                {
                    "User-Agent": UA,
                    "Accept": "*/*",
                    "Accept-Language": "ru,en;q=0.9",
                    "Origin": BASE,
                    "Referer": f"{BASE}/client/photo",
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json",
                }
            )
            for name, value in self.cookies.items():
                s.cookies.set(name, value, domain=".yandex.ru")
            self._tls.s = s
        return s

    def _cid(self) -> str:
        return f"{self.uid}{int(time.time() * 1000)}{random.randint(100, 999)}"

    def api(self, method: str, params=None, retries: int = 4):
        last = None
        for attempt in range(retries):
            body = {"sk": self.sk, "connection_id": self._cid(), "apiMethod": method}
            if params is not None:
                body["requestParams"] = params
            s = self._session()
            try:
                r = s.post(f"{BASE}/models-v2", params={"m": method}, json=body, timeout=180)
            except Exception as exc:  # network hiccup
                last = str(exc)
                time.sleep(2 ** attempt)
                continue

            if r.status_code == 403:
                data = r.json()
                if data.get("error", {}).get("code") == "CKEY":
                    self._set_sk(data["error"]["ckey"])
                    log(" [sk rotated]")
                    continue
            if r.status_code in (401, 403):
                sys.exit(f"Сессия истекла (HTTP {r.status_code}) — обнови cookies.json")
            if r.status_code == 429:
                wait = 30 + attempt * 30
                log(f" [429] {method}: sleep {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                last = f"HTTP {r.status_code}: {r.text[:300]}"
                time.sleep(2 ** attempt)
                continue

            data = r.json()
            if isinstance(data, dict) and data.get("type") == "captcha":
                sys.exit("CAPTCHA! Пройди её в браузере на disk.yandex.ru и повтори.")
            return data
        raise RuntimeError(f"{method}: {last}")

    # -- photoslice ----------------------------------------------------------
    def init_snapshot(self) -> str:
        d = self.api("intapi/photo-init-snapshot")
        psid = d.get("photoslice_id") or d.get("photosliceId")
        if not psid:
            raise RuntimeError(f"нет photoslice_id в ответе: {d}")
        return psid

    def get_clusters(self, psid: str) -> list[dict]:
        snap = self.api(
            "intapi/photo-get-snapshot",
            {"id": psid, "locale": "ru", "amount": 100000, "offset": 0},
        )
        return snap.get("items", [])

    @staticmethod
    def _extract_fetched(resp: dict) -> list[dict]:
        out: list[dict] = []
        clusters = resp.get("clusters")
        if isinstance(clusters, dict):
            for cl in clusters.get("fetched") or []:
                out.extend(cl.get("items") or [])
        resources = resp.get("resources")
        if isinstance(resources, dict):
            out.extend(resources.get("fetched") or [])
        return out

    # -- destination ---------------------------------------------------------
    def list_dest_names(self, dest_dir: str) -> set[str] | None:
        """Names already present in `dest_dir` (an mpfs dir like
        /disk/PhotoBackup), via `mpfs/list`.

        `mpfs/list` returns the folder itself as the first element, which is
        filtered out. Returns None if listing is unavailable, so the caller
        can fall back to server-side 412 handling."""
        names: set[str] = set()
        offset = 0
        limit = 1000
        dest_norm = dest_dir.rstrip("/")
        while True:
            try:
                items = self.api("mpfs/list", {"path": dest_norm, "offset": offset, "limit": limit})
            except Exception as exc:
                if "code\":48" in str(exc) or "service not found" in str(exc).lower():
                    return set()  # folder missing -> nothing present yet
                log(f" [!] не удалось получить листинг {dest_norm}: {exc}")
                return None
            if not isinstance(items, list) or not items:
                return names
            before = len(names)
            for it in items:
                if not isinstance(it, dict):
                    continue
                path = (it.get("path") or "").rstrip("/")
                if path == dest_norm:  # the folder itself
                    continue
                nm = it.get("name") or path.rsplit("/", 1)[-1]
                if nm:
                    names.add(nm)
            if len(items) < limit or len(names) == before:
                return names  # last page, or the method ignores offset
            offset += len(items)

    def fetch_cluster_items(self, psid: str, clusters: list[dict]) -> list[dict]:
        q = {}
        for c in clusters:
            cid = c.get("id") or c.get("cluster_id")
            size = max(int(c.get("size", 0) or 0), 1)
            if cid:
                q[cid] = {"range": [0, max(size - 1, 0)]}
        if not q:
            return []
        resp = self.api(
            "intapi/photo-get-clusters-with-resources",
            {"photosliceId": psid, "clusters": q, "hideScreenshots": False},
        )
        return self._extract_fetched(resp)

    def fetch_cluster_batch(
        self, psid: str, clusters: list[dict], only_unlimited: bool = True
    ) -> list[str]:
        out = []
        for f in self.fetch_cluster_items(psid, clusters):
            if only_unlimited and not is_unlimited(f):
                continue
            raw = f.get("path") or f.get("id") or ""
            if raw:
                out.append(to_mpfs_path(raw))
        return out


# --- caches ------------------------------------------------------------------

def read_lines(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [ln.rstrip("\n") for ln in fh if ln.strip()]


def append_lines(path: str, items) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        for it in items:
            fh.write(it + "\n")
        fh.flush()


def write_lines(path: str, items) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for it in items:
            fh.write(it + "\n")
        fh.flush()


def dedup_file(path: str) -> int:
    """Collapse duplicate lines in a cache file in place. Returns remaining count."""
    items = list(dict.fromkeys(read_lines(path)))
    write_lines(path, items)
    return len(items)


def load_collect_state() -> dict:
    if os.path.exists(COLLECT_STATE):
        try:
            return json.loads(open(COLLECT_STATE, encoding="utf-8").read())
        except Exception:
            pass
    return {}


def save_collect_state(state: dict) -> None:
    with open(COLLECT_STATE, "w", encoding="utf-8") as fh:
        json.dump(state, fh)


# --- collection --------------------------------------------------------------

def collect_batches(bff: BFF, args):
    """Generator: yields a list of new paths per cluster batch, appending to
    the cache and checkpointing progress so it can resume."""
    only_unlimited = not getattr(args, "all_photos", False)
    state = {} if args.refresh else load_collect_state()
    psid = state.get("psid") or bff.init_snapshot()
    clusters = bff.get_clusters(psid)
    total = len(clusters)
    offset = int(state.get("offset", 0)) if not args.refresh else 0
    cached = read_lines(PATHS_CACHE)
    if only_unlimited:
        cached = unlimited_paths_only(cached)
    seen = set(cached) if not args.refresh else set()
    if args.refresh:
        open(PATHS_CACHE, "w").close()
    log(f"[collect] photoslice_id={psid}")
    scope = "только безлимитные (photounlim)" if only_unlimited else "все фото раздела"
    log(f"[collect] охват: {scope}")
    log(f"[collect] кластеров (дней): {total}, старт с #{offset}, уже собрано {len(seen)}")

    batch = max(1, args.collect_batch)
    delay = args.collect_delay
    for i in range(offset, total, batch):
        chunk = clusters[i : i + batch]
        try:
            raw_paths = bff.fetch_cluster_batch(psid, chunk, only_unlimited=only_unlimited)
        except Exception as exc:
            log(f" [!] batch {i}: {exc}")
            continue
        new = []
        for p in raw_paths:
            if p not in seen:
                seen.add(p)
                new.append(p)
        if new:
            append_lines(PATHS_CACHE, new)
        state = {"psid": psid, "offset": min(i + batch, total)}
        save_collect_state(state)
        log(f" [{min(i + batch, total)}/{total}] +{len(new)} (всего {len(seen)})")
        if new:
            yield new
        if delay:
            time.sleep(random.uniform(delay * 0.6, delay * 1.4))


# --- copy --------------------------------------------------------------------

MAX_BATCH = 40  # the server rejects more than 40 operations per request
THROTTLE_ERROR = "TooManyExecuting"  # server: too many async copies already queued


class RateGate:
    """Serialises copy requests across workers.

    Enforces a minimum interval between requests and lets any worker put the
    whole pipeline into a cooldown when the server reports that too many
    operations are already executing (TooManyExecutingOperationsError). The
    interval also adapts: it grows on throttle and slowly relaxes when copies
    succeed, so one busy period makes the whole run gentler."""

    def __init__(self, min_interval: float, max_interval: float = 120.0):
        self.base = max(0.0, min_interval)
        self.max_interval = max(max_interval, self.base)
        self.min_interval = self.base
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_at:
                    self._next_at = now + self.min_interval
                    return
                sleep_for = self._next_at - now
            time.sleep(min(sleep_for, 1.0))

    def cooldown(self, seconds: float) -> None:
        with self._lock:
            self._next_at = max(self._next_at, time.monotonic() + seconds)

    def slowdown(self) -> None:
        """Server is throttling us: double the interval between requests."""
        with self._lock:
            self.min_interval = min(self.max_interval, (self.min_interval or 1.0) * 2)

    def relax(self) -> None:
        """Copies are flowing: ease the interval back toward the base value."""
        with self._lock:
            self.min_interval = max(self.base, self.min_interval * 0.7)


def parse_copy_results(ops: list[dict], results):
    """Classify a copy response.

    Returns (ok, skipped, failed_srcs, succeeded_srcs, errors) where `errors`
    is a Counter of error titles for the failed operations."""
    ok = skip = 0
    failed_srcs: list[str] = []
    succeeded: list[str] = []
    errors: "Counter" = Counter()
    if isinstance(results, list) and results:
        for idx, r in enumerate(results):
            src = ops[idx]["src"] if idx < len(ops) else None
            if isinstance(r, dict) and r.get("error"):
                err = r["error"]
                text = json.dumps(err, ensure_ascii=False).lower()
                if "already exist" in text or "exists" in text or "уже" in text:
                    skip += 1
                    if src:
                        succeeded.append(src)  # already on the destination
                else:
                    title = str(err.get("title") or err.get("code") or err)[:90]
                    errors[title] += 1
                    if src:
                        failed_srcs.append(src)
            else:
                ok += 1
                if src:
                    succeeded.append(src)
    else:  # no per-op detail: assume the whole batch went through
        ok = len(ops)
        succeeded = [o["src"] for o in ops]
    return ok, skip, failed_srcs, succeeded, errors


def send_copy(bff: BFF, ops: list[dict], wid: int, quiet: bool = False):
    """Send one copy request. On 'Too many items' split the batch in half and
    retry, so a too-large batch is recovered instead of lost."""
    try:
        resp = bff.api(COPY_METHOD, {"operations": ops})
    except Exception as exc:
        if "Too many items" in str(exc) and len(ops) > 1:
            mid = len(ops) // 2
            log(f" [w{wid}] пачка {len(ops)} слишком большая → делю на {mid}+{len(ops) - mid}")
            a = send_copy(bff, ops[:mid], wid, quiet)
            b = send_copy(bff, ops[mid:], wid, quiet)
            return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], a[4] + b[4])
        if not quiet:
            log(f" [w{wid}] запрос не прошёл ({len(ops)}): {str(exc)[:200]}")
        err = Counter({f"request: {str(exc)[:60]}": len(ops)})
        return 0, 0, [o["src"] for o in ops], [], err
    return parse_copy_results(ops, resp)


def copy_worker(
    bff: BFF,
    wid: int,
    q: "queue.Queue",
    dest: str,
    args,
    counters: dict,
    lock: threading.Lock,
    gate: RateGate,
):
    while True:
        task = q.get()
        try:
            if task is None:
                return
            paths, attempt = task
            total_ok = total_skip = 0
            failed_srcs: list[str] = []
            errors: "Counter" = Counter()
            # Retries happen *here*, not by re-queueing: a worker that put a
            # retry back into a bounded queue would deadlock (it is the same
            # thread that has to drain it).
            while paths:
                ops = [{"src": src, "dst": dest_for(dest, src)} for src in paths]
                gate.wait()
                ok, skip, failed_srcs, succeeded, errors = send_copy(bff, ops, wid)
                total_ok += ok
                total_skip += skip
                with lock:
                    counters["copied"] += ok
                    counters["skipped"] += skip
                    if succeeded:
                        append_lines(COPIED_CACHE, succeeded)
                    if errors:
                        counters["errors"].update(errors)
                    done, skp = counters["copied"], counters["skipped"]

                if not failed_srcs or attempt >= args.retries:
                    break

                throttled = any(THROTTLE_ERROR in t for t in errors)
                if throttled:
                    # the account still has too many copies executing: back off
                    # hard and slow the whole pipeline down
                    wait = min(args.retry_max, args.throttle_base * (2 ** attempt))
                    wait *= random.uniform(0.9, 1.1)
                    gate.slowdown()
                    gate.cooldown(wait)
                else:
                    wait = min(args.retry_max, args.retry_base * (2 ** attempt))
                    wait *= random.uniform(0.8, 1.3)
                sample = f" | причины: {dict(errors.most_common(3))}" if errors else ""
                log(
                    f" [w{wid}] не прошло {len(failed_srcs)} из {len(paths)} "
                    f"(попытка {attempt + 1}/{args.retries}), повтор через {wait:.0f}с{sample}"
                )
                time.sleep(wait)
                paths = failed_srcs
                attempt += 1

            if failed_srcs:
                append_lines(FAILED_CACHE, failed_srcs)
                with lock:
                    counters["failed"] += len(failed_srcs)
                    bad = counters["failed"]
                sample = f" | причины: {dict(errors.most_common(3))}" if errors else ""
                log(
                    f" [w{wid}] ok={total_ok} skip={total_skip} fail={len(failed_srcs)} | "
                    f"total copied={done} skipped={skp} failed={bad}{sample}"
                )
            else:
                gate.relax()
                log(
                    f" [w{wid}] ok={total_ok} skip={total_skip} fail=0 | "
                    f"total copied={done} skipped={skp} failed={counters['failed']}"
                )
        finally:
            q.task_done()


def dest_for(dest: str, mpfs_src: str) -> str:
    """Build the mpfs destination path. Normal folders live under /disk, so a
    plain '/PhotoBackup' becomes '/disk/PhotoBackup/<name>'."""
    name = mpfs_src.rsplit("/", 1)[-1]
    base = dest.rstrip("/")
    if not base.startswith("/disk"):
        base = "/disk" + (base if base.startswith("/") else "/" + base)
    return base + "/" + name


def run_copy(bff: BFF, batches, dest: str, args) -> None:
    copied_before = set(read_lines(COPIED_CACHE))
    dest_dir = dest_for(dest, "x").rsplit("/", 1)[0]
    claimed_names: set[str] = set()  # within-run guard against name collisions
    if getattr(args, "dest_check", True):
        existing = bff.list_dest_names(dest_dir)
        if existing is not None:
            claimed_names |= existing
            log(f"[copy] в {dest_dir} уже лежит {len(existing)} файлов — пропускаю их")
        else:
            log("[copy] листинг недоступен, полагаюсь на ответ 412 'already exists'")
    counters = {"copied": 0, "failed": 0, "skipped": 0, "errors": Counter()}
    lock = threading.Lock()
    gate = RateGate(args.copy_delay)
    q: queue.Queue = queue.Queue(maxsize=max(args.workers * 4, 8))
    threads = [
        threading.Thread(
            target=copy_worker,
            args=(bff, i + 1, q, dest, args, counters, lock, gate),
            daemon=True,
        )
        for i in range(args.workers)
    ]
    for t in threads:
        t.start()

    log(
        f"[copy] {args.workers} воркеров -> {dest_for(dest, 'x').rsplit('/', 1)[0]} "
        f"(пачка {args.copy_batch}, пауза {args.copy_delay:.0f}с, повторов {args.retries})"
    )
    enqueued = 0
    try:
        for batch in batches:
            fresh = []
            for p in batch:
                if p in copied_before:
                    continue
                name = p.rsplit("/", 1)[-1]
                if name in claimed_names:  # name already claimed in this run
                    continue
                claimed_names.add(name)
                fresh.append(p)
            for p in fresh:
                copied_before.add(p)
            if not fresh:
                continue
            if args.limit:
                remaining = args.limit - enqueued
                if remaining <= 0:
                    break
                fresh = fresh[:remaining]
            q.put((fresh, 0))
            enqueued += len(fresh)
            if args.limit and enqueued >= args.limit:
                log(f"[copy] достигнут лимит {args.limit} (поставлено {enqueued}), прекращаю сбор")
                break
    except KeyboardInterrupt:
        log("\n[copy] прерывание, останавливаюсь...")
    finally:
        q.join()
        for _ in threads:
            q.put(None)
        for t in threads:
            t.join()
    log(
        f"[copy] готово: copied={counters['copied']} "
        f"skipped={counters['skipped']} failed={counters['failed']}"
    )
    if counters["errors"]:
        log(f"[copy] причины ошибок: {dict(counters['errors'].most_common(5))}")
    if counters["failed"]:
        left = dedup_file(FAILED_CACHE)
        log(f"[copy] неудачные файлы в {FAILED_CACHE} (всего {left}); перезапуск: --retry-failed")


# --- commands ----------------------------------------------------------------

def cmd_list(bff: BFF, args) -> None:
    n_files = 0
    for batch in collect_batches(bff, args):
        n_files += len(batch)
    log(f"[list] новых файлов: {n_files}, кэш: {PATHS_CACHE}")


def cmd_probe(bff: BFF, args) -> None:
    state = load_collect_state()
    psid = state.get("psid") or bff.init_snapshot()
    clusters = bff.get_clusters(psid)
    only_unlimited = not getattr(args, "all_photos", False)
    src = None
    for i in range(0, min(len(clusters), 800), 20):
        paths = bff.fetch_cluster_batch(psid, clusters[i : i + 20], only_unlimited=only_unlimited)
        if paths:
            src = paths[0]
            break
        time.sleep(1.5)
    if not src:
        sys.exit("Не нашёл ни одного файла для пробника")
    dst = dest_for(args.dest, src)
    op = {"src": src, "dst": dst}
    log("--- PROBE ---")
    log(f"method: {COPY_METHOD}")
    log(f"src:    {src}")
    log(f"dst:    {dst}")
    log(f"body:   {json.dumps({'operations': [op]}, ensure_ascii=False)}")
    try:
        resp = bff.api(COPY_METHOD, {"operations": [op]})
        log(f"resp:   {json.dumps(resp, ensure_ascii=False)[:2000]}")
    except Exception as exc:
        log(f"ERR:    {exc}")
    log("[probe] копирование асинхронное — файл появится в папке через несколько секунд")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--list", action="store_true", help="собрать список файлов (безопасно)")
    ap.add_argument("--probe", action="store_true", help="проверить формат запроса копирования")
    ap.add_argument("--copy", action="store_true", help="конвейер: сбор + копирование")
    ap.add_argument("--from-cache", action="store_true", help="копировать только из кэша (без сбора)")
    ap.add_argument(
        "--retry-failed",
        dest="retry_failed",
        action="store_true",
        help="повторить только файлы из .failed.txt (без сбора)",
    )
    ap.add_argument("--refresh", action="store_true", help="игнорировать кэши, начать заново")
    ap.add_argument(
        "--all-photos",
        dest="all_photos",
        action="store_true",
        help="копировать все фото раздела, а не только безлимитные (по умолчанию только безлимит)",
    )
    ap.add_argument("--dest", default="/PhotoBackup", help="папка-назначение (по умолчанию /PhotoBackup)")
    ap.add_argument("--limit", type=int, default=None, help="ограничить число скопированных файлов")
    ap.add_argument(
        "--no-dest-check",
        dest="dest_check",
        action="store_false",
        help="не читать листинг папки-назначения (по умолчанию читается)",
    )
    # tuning
    ap.add_argument("--collect-batch", type=int, default=15, help="кластеров за запрос (default 15)")
    ap.add_argument("--collect-delay", type=float, default=1.5, help="пауза между запросами сбора, сек (default 1.5)")
    ap.add_argument(
        "--copy-batch",
        type=int,
        default=10,
        help="файлов за запрос копирования, максимум 40 (default 10 — меньше операций в полёте)",
    )
    ap.add_argument(
        "--copy-delay",
        type=float,
        default=5.0,
        help="минимальная пауза между запросами копирования (общая на всех воркеров), сек (default 5)",
    )
    ap.add_argument("--workers", type=int, default=1, help="воркеров копирования (default 1 — мягкий режим)")
    ap.add_argument("--retries", type=int, default=6, help="повторов для упавших файлов (default 6)")
    ap.add_argument("--retry-base", type=float, default=10.0, help="базовая пауза перед повтором, сек (default 10)")
    ap.add_argument(
        "--throttle-base",
        type=float,
        default=30.0,
        help="базовая пауза при 'слишком много копирований' (TooManyExecutingOperationsError), сек (default 30)",
    )
    ap.add_argument("--retry-max", type=float, default=600.0, help="максимальная пауза перед повтором, сек (default 600)")
    args = ap.parse_args()

    if args.copy_batch > MAX_BATCH:
        log(f"[cfg] --copy-batch {args.copy_batch} > {MAX_BATCH}, снижаю до {MAX_BATCH}")
        args.copy_batch = MAX_BATCH

    if not any([args.list, args.probe, args.copy, args.retry_failed]):
        ap.print_help()
        return 0

    cookies = load_cookies(COOKIES_FILE)
    uid, sk = authorize(cookies)
    log(f"[auth] uid={uid}, sk OK")
    bff = BFF(cookies, uid, sk)

    if args.list:
        cmd_list(bff, args)
        return 0

    if args.probe:
        cmd_probe(bff, args)
        return 0

    if args.retry_failed:
        failed = read_lines(FAILED_CACHE)
        if not args.all_photos:
            failed = unlimited_paths_only(failed)
        done_before = set(read_lines(COPIED_CACHE))
        failed = [p for p in failed if p not in done_before]
        if not failed:
            log(f"[retry] в {FAILED_CACHE} нет нерешённых файлов")
            return 0
        log(f"[retry] {len(failed)} файлов из {FAILED_CACHE} -> {args.dest}")
        batches = (failed[i : i + args.copy_batch] for i in range(0, len(failed), args.copy_batch))
        run_copy(bff, batches, args.dest, args)
        done_after = set(read_lines(COPIED_CACHE))
        rest = [p for p in dict.fromkeys(read_lines(FAILED_CACHE)) if p not in done_after]
        write_lines(FAILED_CACHE, rest)
        log(f"[retry] осталось упавших: {len(rest)} (файл {FAILED_CACHE})")
        return 0

    # --copy
    if args.from_cache:
        paths = read_lines(PATHS_CACHE)
        if not args.all_photos:
            paths = unlimited_paths_only(paths)
        log(f"[cache] {len(paths)} путей из {PATHS_CACHE}")
        batches = (paths[i : i + args.copy_batch] for i in range(0, len(paths), args.copy_batch))
    else:
        def stream():
            buf: list[str] = []
            for batch in collect_batches(bff, args):
                buf.extend(batch)
                while len(buf) >= args.copy_batch:
                    yield buf[: args.copy_batch]
                    buf = buf[args.copy_batch:]
            if buf:
                yield buf

        batches = stream()

    run_copy(bff, batches, args.dest, args)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
