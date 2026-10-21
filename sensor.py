"""Simulated weather station. Picks a home node by hashing its ID; fails over to the next node if down."""
import argparse, random, time, zlib
from node import http_post

ap = argparse.ArgumentParser()
ap.add_argument("--id", required=True)
ap.add_argument("--nodes", default="5001,5002,5003")
ap.add_argument("--interval", type=float, default=2)
ap.add_argument("--home", type=int, default=None, help="preferred node port (default: hash of ID)")
ap.add_argument("--start-temp", type=float, default=None, help="e.g. 36 to simulate a heatwave")
a = ap.parse_args()

nodes = [int(x) for x in a.nodes.split(",")]
start = nodes.index(a.home) if a.home in nodes else zlib.crc32(a.id.encode()) % len(nodes)   # home node
temp = a.start_temp if a.start_temp else random.uniform(18, 34)
hum = random.uniform(40, 80); pres = random.uniform(1000, 1020)
seq = int(time.time())          # unique across sensor restarts (keys never collide)
last_port = None
while True:
    seq += 1
    temp += random.uniform(-0.5, 0.5); hum = min(100, max(10, hum + random.uniform(-2, 2)))
    pres += random.uniform(-0.3, 0.3)
    reading = {"station": a.id, "seq": seq, "ts": time.time(),
               "temp": round(temp, 2), "humidity": round(hum, 1), "pressure": round(pres, 1)}
    for i in range(len(nodes)):
        port = nodes[(start + i) % len(nodes)]
        reading["via"] = port
        try:
            res = http_post(f"http://localhost:{port}/ingest", reading)
            if last_port is not None and port != last_port:       # report failover / fail-back
                try:
                    http_post(f"http://localhost:{port}/event", {
                        "id": f"{time.time():.3f}-{a.id}-{seq}", "ts": time.time(), "level": "warn",
                        "node": port, "msg": f"Sensor {a.id} switched from node {last_port} to node {port}"})
                except Exception: pass
            last_port = port
            print(f"[{a.id}] #{seq} temp={reading['temp']} -> node {port} (lc={res['lc']})", flush=True)
            break
        except Exception:
            print(f"[{a.id}] node {port} unreachable, failing over...", flush=True)
    time.sleep(a.interval)