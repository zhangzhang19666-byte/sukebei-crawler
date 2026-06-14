#!/usr/bin/env python3
import asyncio
import json
import re
import random
import argparse
from pathlib import Path
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# --- Configuration ---
H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}
BASE_URL   = "https://sukebei.nyaa.si/view/{}"
STATE_FILE = "crawler_state.json"
RESULTS_FILE = "nyaa_ec2_results.jsonl"

# ---------------------------------------------------------------------------
def parse_html(html, id_val):
    soup = BeautifulSoup(html, "html.parser")
    if soup.find("div", class_="alert-danger"):
        return None
    title_tag = soup.find("h3", class_="panel-title")
    if not title_tag:
        return None
    res = {"id": id_val, "title": title_tag.get_text(strip=True)}
    magnet_tag = soup.find("a", href=re.compile(r"^magnet:\?"))
    res["magnet"] = magnet_tag["href"] if magnet_tag else None
    if res["magnet"]:
        m = re.search(r"btih:([a-fA-F0-9]{40})", res["magnet"])
        res["info_hash"] = m.group(1).lower() if m else None
    ts_tag = soup.find(attrs={"data-timestamp": True})
    if ts_tag:
        try:
            res["uploaded_at"] = datetime.fromtimestamp(
                int(ts_tag["data-timestamp"]), tz=timezone.utc).isoformat()
        except Exception:
            pass
    def get_int(id_attr):
        tag = soup.find(id=id_attr)
        try:   return int(tag.get_text(strip=True)) if tag else 0
        except: return 0
    res["seeders"]  = get_int("seeders")
    res["leechers"] = get_int("leechers")
    for row in soup.select(".panel-body .row"):
        cols = row.find_all("div", recursive=False)
        if len(cols) < 2:
            continue
        key = cols[0].get_text(strip=True).rstrip(":")
        val = cols[1].get_text(strip=True)
        if   key == "Category":              res["category"]    = val
        elif key == "Submitter":             res["submitter"]   = val
        elif key in ("File size", "Size"):   res["size"]        = val
        elif key == "Completed":
            try:   res["completed"] = int(val)
            except: res["completed"] = 0
        elif key == "Information":
            a = cols[1].find("a")
            res["information"] = a["href"] if a else val
    d = soup.find(id="torrent-description")
    res["description"] = d.get_text(strip=True) if d else None
    return res

# ---------------------------------------------------------------------------
async def fetch_one(id_val, session, min_delay, max_delay):
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    try:
        resp = await session.get(BASE_URL.format(id_val), timeout=15)
        if resp.status_code == 404:
            return id_val, None, "404"
        if resp.status_code == 429:
            return id_val, None, "429"
        resp.raise_for_status()
        data = parse_html(resp.text, id_val)
        return id_val, data, "ok" if data else "parse_fail"
    except Exception as e:
        return id_val, None, str(e)[:50]

# ---------------------------------------------------------------------------
def save_state(progress, count):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"progress": progress, "count": count}, f)
    except Exception as e:
        print(f" [!] Failed to save state: {e}")

def load_state():
    try:
        if Path(STATE_FILE).exists():
            with open(STATE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {"progress": 0, "count": 0}

# ---------------------------------------------------------------------------
async def run_crawler(start_id, end_id, workers, min_delay, max_delay,
                      proxy, output_file, batch_size):

    # --- resume logic: if state has a valid progress, always prefer it ---
    state = load_state()
    if state["progress"] > 0:
        print(f"[*] Resuming from saved progress: {state['progress']}")
        start_id = state["progress"]

    current_count = state.get("count", 0)

    # batch limit
    target_end = max(end_id, start_id - batch_size + 1) if batch_size > 0 else end_id

    if start_id < target_end:
        print("[*] Crawler already reached the end ID.")
        return

    print(f"[*] Crawling from {start_id} down to {target_end}  (global end: {end_id})")
    print(f"[*] workers={workers}  delay={min_delay}-{max_delay}s  batch={batch_size}")

    queue      = asyncio.Queue(maxsize=workers * 2)
    stop_event = asyncio.Event()
    lock       = asyncio.Lock()

    processed_count = 0
    found_count     = 0

    ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = f"nyaa_{ts}.jsonl"
    fh          = open(output_file, "w", encoding="utf-8")

    # -----------------------------------------------------------------------
    async def worker_task(session):
        nonlocal found_count, processed_count
        while not stop_event.is_set():
            # Fix: use shield+wait instead of wait_for on queue.get()
            # to avoid corrupting queue internal state on timeout
            get_coro = asyncio.ensure_future(queue.get())
            done, _ = await asyncio.wait({get_coro}, timeout=1.0)
            if not done:
                get_coro.cancel()
                try:
                    await get_coro
                except asyncio.CancelledError:
                    pass
                continue

            id_val = get_coro.result()

            _, data, status_msg = await fetch_one(id_val, session, min_delay, max_delay)
            do_backoff = (status_msg == "429")

            async with lock:
                processed_count += 1
                if data:
                    found_count += 1
                    record = data
                    record["status"] = "ok"
                else:
                    record = {"id": id_val, "status": status_msg}

                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

                if do_backoff:
                    print(f" [!] 429 on #{id_val}, sleeping 30s...")
                    fh.flush()

                if processed_count % 50 == 0:
                    fh.flush()
                    save_state(id_val, current_count + found_count)
                    print(f" [{processed_count}] id={id_val} found={found_count} status={status_msg}")

            queue.task_done()

            if do_backoff:
                await asyncio.sleep(30)

    # -----------------------------------------------------------------------
    session_kwargs = {"headers": H, "impersonate": "chrome110"}
    if proxy:
        session_kwargs["proxies"] = {"http": proxy, "https": proxy}

    try:
        async with AsyncSession(**session_kwargs) as session:
            worker_tasks = [asyncio.create_task(worker_task(session))
                            for _ in range(workers)]

            async def producer():
                for i in range(start_id, target_end - 1, -1):
                    await queue.put(i)

            prod_task = asyncio.create_task(producer())
            await prod_task    # wait until all IDs are enqueued
            await queue.join() # wait until all enqueued items are processed

            stop_event.set()
            for t in worker_tasks:
                t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)

            # flush and save only after all workers have stopped
            fh.flush()
            save_state(target_end - 1, current_count + found_count)
    finally:
        fh.close()

    print(f"[*] Done. processed={processed_count} found={found_count} file={output_file}")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start",      type=int,   default=3806848)
    parser.add_argument("--end",        type=int,   default=92)
    parser.add_argument("--workers",    type=int,   default=5)
    parser.add_argument("--proxy",      type=str,   default=None)
    parser.add_argument("--min-delay",  type=float, default=0.8)
    parser.add_argument("--max-delay",  type=float, default=1.1)
    parser.add_argument("--output",     type=str,   default=RESULTS_FILE)
    parser.add_argument("--batch-size", type=int,   default=1100)
    args = parser.parse_args()

    asyncio.run(run_crawler(
        args.start, args.end, args.workers,
        args.min_delay, args.max_delay,
        args.proxy, args.output, args.batch_size,
    ))
