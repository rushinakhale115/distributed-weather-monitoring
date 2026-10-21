"""Launch 3 nodes, 5 sensors, dashboard.
  python run_all.py            normal run (then use the commands below to kill/start nodes live)
  python run_all.py --fresh    delete saved data first
  python run_all.py --chaos    auto: kill the COORDINATOR (node 5003) at 20s, restart it at 80s

Type these in the same terminal while it runs:
  kill 5003      -> stop that node   (dashboard turns it RED; if it is the coordinator, a new election happens)
  start 5003     -> bring it back    (turns GREEN, syncs data, wins election again)
  quit           -> stop everything
"""
import glob, os, subprocess, sys, threading, time
py = sys.executable
if "--fresh" in sys.argv:
    for f in glob.glob("data/*.jsonl"): os.remove(f)

PEERS = {5001: "5002,5003", 5002: "5001,5003", 5003: "5001,5002"}
procs = {}
def start(p):
    if p in procs and procs[p].poll() is None:
        print(f">> node {p} is already running"); return
    procs[p] = subprocess.Popen([py, "node.py", "--port", str(p), "--peers", PEERS[p]])
    print(f">> node {p} STARTED")
def kill(p):
    if p in procs and procs[p].poll() is None:
        procs[p].kill(); procs[p].wait(); print(f">> node {p} KILLED")
    else:
        print(f">> node {p} is not running")

for p in PEERS: start(p)
time.sleep(1)
HOMES = {"Pune": 5001, "Mumbai": 5002, "Nashik": 5003, "Satara": 5001, "Nagpur": 5002}   # spread evenly over nodes
extra = [subprocess.Popen([py, "sensor.py", "--id", s, "--home", str(h)]) for s, h in HOMES.items() if s != "Nagpur"]
extra.append(subprocess.Popen([py, "sensor.py", "--id", "Nagpur", "--home", "5002", "--start-temp", "36"]))   # heatwave -> alert
extra.append(subprocess.Popen([py, "dashboard.py"]))
print("\nDashboard: http://localhost:8000   |   commands: kill <port>, start <port>, quit\n")

stop = threading.Event()
def chaos():
    if stop.wait(20): return
    print("\n*** CHAOS: killing coordinator node 5003 (back in 60s) ***\n"); kill(5003)
    if stop.wait(60): return
    print("\n*** CHAOS: restarting node 5003 ***\n"); start(5003)
if "--chaos" in sys.argv: threading.Thread(target=chaos, daemon=True).start()

try:
    while True:
        line = sys.stdin.readline()
        if not line: stop.wait(); break                 # no terminal input (e.g. background run)
        cmd = line.split()
        if not cmd: continue
        if cmd[0] == "quit": break
        if cmd[0] in ("kill", "start") and len(cmd) == 2 and cmd[1].isdigit() and int(cmd[1]) in PEERS:
            (kill if cmd[0] == "kill" else start)(int(cmd[1]))
        else:
            print(">> usage: kill <5001|5002|5003>, start <5001|5002|5003>, quit")
except KeyboardInterrupt:
    pass
finally:
    stop.set()
    for p in list(procs.values()) + extra: p.terminate()