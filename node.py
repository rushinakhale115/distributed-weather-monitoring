"""Storage node: receives readings, replicates to peers, detects failures, re-syncs on recovery,
persists to disk, elects a coordinator (Bully algorithm) and keeps a cluster event log."""
import argparse, collections, json, os, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ALERT_TEMP = 35.0        # deg C: coordinator raises a HEAT ALERT above this
OFFLINE_AFTER = 10       # seconds without data -> sensor considered OFFLINE

def http_post(url, obj, timeout=1.5):
    req = urllib.request.Request(url, json.dumps(obj).encode(), {"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())

def http_get(url, timeout=1.5):
    return json.loads(urllib.request.urlopen(url, timeout=timeout).read())

class Node:
    def __init__(self, port, peers, data_dir="data"):
        self.port, self.peers = port, peers
        self.lock = threading.Lock()
        self.data = {}                       # "station:seq" -> reading
        self.clock = 0                       # Lamport logical clock
        self.alive = {p: False for p in peers}
        self.was_down = set()                # peers that went down after being seen up
        self.leader = None                   # current coordinator (Bully algorithm)
        self.electing, self.last_election = False, 0
        self.events = collections.OrderedDict()   # cluster event log (replicated)
        self.ev_n = 0
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"node_{port}.jsonl")
        self.load()

    # ---------- persistence ----------
    def load(self):
        if not os.path.exists(self.path): return
        with open(self.path) as f:
            for line in f:
                try: r = json.loads(line)
                except ValueError: continue          # ignore half-written last line
                self.data[f"{r['station']}:{r['seq']}"] = r
                self.clock = max(self.clock, r.get("lc", 0))
        print(f"[node {self.port}] recovered {len(self.data)} records from disk", flush=True)

    # ---------- data + replication ----------
    def store(self, r):
        key = f"{r['station']}:{r['seq']}"
        with self.lock:
            self.clock = max(self.clock, r.get("lc", 0)) + 1
            if key in self.data:
                return False
            r.setdefault("lc", self.clock)
            self.data[key] = r
            with open(self.path, "a") as f:          # append-only log on disk
                f.write(json.dumps(r) + "\n")
            return True

    def _fanout(self, path, obj):
        for p in self.peers:
            def send(p=p):
                try: http_post(f"http://localhost:{p}{path}", obj)
                except Exception: pass       # peer down; it will catch up via sync
            threading.Thread(target=send, daemon=True).start()

    def replicate(self, r):
        self._fanout("/replicate", r)

    # ---------- cluster event log ----------
    def add_event(self, msg, level="info", ev=None):
        with self.lock:
            if ev is None:
                self.ev_n += 1
                ev = {"id": f"{time.time():.3f}-{self.port}-{self.ev_n}", "ts": time.time(),
                      "msg": msg, "level": level, "node": self.port}
            if ev["id"] in self.events:
                return False
            self.events[ev["id"]] = ev
            while len(self.events) > 200: self.events.popitem(last=False)
        print(f"[node {self.port}] EVENT: {ev['msg']}", flush=True)
        self._fanout("/event", ev)
        return True

    # ---------- Bully election ----------
    def start_election(self):
        with self.lock:
            if self.electing: return
            self.electing = True
        try:
            answered = False
            for p in (q for q in self.peers if q > self.port):   # ask higher-ID nodes
                try: http_post(f"http://localhost:{p}/election", {"from": self.port}); answered = True
                except Exception: pass
            if not answered:                                      # nobody higher is alive -> I win
                was_me = (self.leader == self.port)
                self.leader = self.port
                if not was_me:
                    self.add_event(f"Node {self.port} elected COORDINATOR (Bully algorithm)", "warn")
                for p in self.peers:
                    try: http_post(f"http://localhost:{p}/coordinator", {"leader": self.port})
                    except Exception: pass
        finally:
            self.last_election = time.time()
            self.electing = False

    # ---------- background loops ----------
    def monitor(self):
        """Heartbeat peers; sync on (re)appearance; trigger election when coordinator is missing."""
        time.sleep(1.5)
        while True:
            for p in self.peers:
                try:
                    http_get(f"http://localhost:{p}/health")
                    if not self.alive[p]:
                        print(f"[node {self.port}] peer {p} UP -> syncing", flush=True)
                        for r in http_get(f"http://localhost:{p}/data"):
                            self.store(r)
                        if p in self.was_down:
                            self.was_down.discard(p)
                            self.add_event(f"Node {p} is back UP and synced", "info")
                    self.alive[p] = True
                except Exception:
                    if self.alive[p]:
                        print(f"[node {self.port}] peer {p} DOWN", flush=True)
                        self.was_down.add(p)
                        self.add_event(f"Node {p} is DOWN (heartbeat failed)", "error")
                    self.alive[p] = False
            dead_leader = self.leader is not None and self.leader != self.port and not self.alive.get(self.leader, False)
            if (self.leader is None or dead_leader) and time.time() - self.last_election > 3:
                if dead_leader:
                    self.add_event(f"Coordinator {self.leader} unreachable - starting election", "error")
                threading.Thread(target=self.start_election, daemon=True).start()
            time.sleep(2)

    def coordinator_duties(self):
        """Only the elected coordinator raises alerts, so there are no duplicate alerts."""
        heat, online = {}, {}
        while True:
            time.sleep(3)
            if self.leader != self.port:
                heat, online = {}, {}
                continue
            latest = {}
            with self.lock:
                for r in self.data.values():
                    if r["station"] not in latest or r["ts"] > latest[r["station"]]["ts"]:
                        latest[r["station"]] = r
            now = time.time()
            for st, r in latest.items():
                t = r["temp"]
                hot = True if t > ALERT_TEMP else (False if t < ALERT_TEMP - 1 else heat.get(st, False))
                if hot and not heat.get(st):
                    self.add_event(f"HEAT ALERT: {st} at {t}°C (raised by coordinator {self.port})", "error")
                if heat.get(st) and not hot:
                    self.add_event(f"{st} cooled down to {t}°C", "info")
                heat[st] = hot
                up = now - r["ts"] <= OFFLINE_AFTER
                if not up and online.get(st, True):
                    self.add_event(f"Sensor {st} is OFFLINE (no data for {OFFLINE_AFTER}s)", "error")
                if up and online.get(st) is False:
                    self.add_event(f"Sensor {st} is back ONLINE", "info")
                online[st] = up

def make_handler(node):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _send(self, obj, code=200):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers(); self.wfile.write(body)
        def do_POST(self):
            r = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/election":
                threading.Thread(target=node.start_election, daemon=True).start()
                return self._send({"ok": True, "node": node.port})
            if self.path == "/coordinator":
                node.leader = r["leader"]
                return self._send({"ok": True})
            if self.path == "/event":
                node.add_event(None, ev=r)
                return self._send({"ok": True})
            is_new = node.store(r)               # /ingest from sensor or /replicate from peer
            if self.path == "/ingest" and is_new:
                node.replicate(r)
            self._send({"ok": True, "node": node.port, "lc": node.clock})
        def do_GET(self):
            if self.path == "/health":
                self._send({"node": node.port, "clock": node.clock, "count": len(node.data),
                            "leader": node.leader, "peers": {str(k): v for k, v in node.alive.items()}})
            elif self.path == "/data":
                with node.lock: self._send(list(node.data.values()))
            elif self.path == "/events":
                with node.lock: self._send(list(node.events.values()))
            else:
                self._send({"error": "not found"}, 404)
    return H

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--peers", default="")
    ap.add_argument("--data-dir", default="data")
    a = ap.parse_args()
    peers = [int(x) for x in a.peers.split(",") if x]
    n = Node(a.port, peers, a.data_dir)
    n.add_event(f"Node {a.port} started ({len(n.data)} records loaded from disk)", "info")
    threading.Thread(target=n.monitor, daemon=True).start()
    threading.Thread(target=n.coordinator_duties, daemon=True).start()
    print(f"[node {a.port}] started, peers={peers}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", a.port), make_handler(n)).serve_forever()