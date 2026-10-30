"""Count files per outlet folder in the scraper-media container (read-only listing, no downloads).

Shards = <run>/ChIJ<c> for every run in --runs and every base64url char c; this slice takes shard i where
i % slices == slice. Writes <out>: run, place_id, photos (<pid>_N.jpg), files (all files).
"""
import argparse, collections, concurrent.futures as cf, csv, gzip, os, re, time, urllib.parse, urllib.request

C = "https://micromarket.blob.core.windows.net/scraper-media"
CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
NAME = re.compile(r"<Blob><Name>([^<]*)</Name>")
NEXT = re.compile(r"<NextMarker>([^<]*)</NextMarker>")


def get(url):
    for i in range(6):
        try:
            return urllib.request.urlopen(url, timeout=120).read().decode("utf-8")
        except Exception:
            if i == 5:
                raise
            time.sleep(2 ** i)


def list_shard(prefix, sas):
    counts, marker, n = collections.defaultdict(lambda: [0, 0]), "", 0
    while True:
        s = get(f"{C}?restype=container&comp=list&maxresults=5000&prefix={urllib.parse.quote(prefix)}"
                + (f"&marker={urllib.parse.quote(marker)}" if marker else "") + "&" + sas)
        for name in NAME.findall(s):
            parts = name.split("/")
            if len(parts) != 3:
                continue
            run, pid, f = parts
            c = counts[(run, pid)]
            c[1] += 1
            if f.startswith(pid + "_") and f.endswith(".jpg"):
                c[0] += 1
            n += 1
        m = NEXT.search(s)
        marker = m.group(1) if m else ""
        if not marker:
            return counts, n


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--runs", required=True)
    a.add_argument("--slice", type=int, default=0)
    a.add_argument("--slices", type=int, default=1)
    a.add_argument("--workers", type=int, default=64)
    a.add_argument("--out", required=True)
    o = a.parse_args()
    sas = os.environ["AZURE_STORAGE_SAS"].strip().lstrip("?")
    shards = [f"{r.strip().rstrip('/')}/ChIJ{c}" for r in o.runs.split(",") if r.strip() for c in CHARS]
    mine = shards[o.slice::o.slices]
    t, total, rows = time.time(), 0, []
    with cf.ThreadPoolExecutor(o.workers) as ex:
        for i, (counts, n) in enumerate(ex.map(lambda p: list_shard(p, sas), mine), 1):
            total += n
            rows += [(r, p, c[0], c[1]) for (r, p), c in counts.items()]
            if i % 100 == 0:
                print(f"{i}/{len(mine)} shards, {total:,} blobs, {total / (time.time() - t):,.0f}/s", flush=True)
    with gzip.open(o.out, "wt", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["run", "place_id", "photos", "files"])
        w.writerows(rows)
    print(f"done: {len(mine)} shards, {total:,} blobs, {len(rows):,} folders in {time.time() - t:.0f}s "
          f"({total / (time.time() - t):,.0f} blobs/s)", flush=True)


if __name__ == "__main__":
    main()
