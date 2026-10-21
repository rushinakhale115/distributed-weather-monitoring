"""Web dashboard (http://localhost:8000): merges data from all nodes.
Shows coordinator, node health, sync status, sensor status, alerts, stats, metric chart, event log, CSV export."""
import json, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from node import http_get, ALERT_TEMP, OFFLINE_AFTER

NODES = [5001, 5002, 5003]
HISTORY = 40               # readings per station shown in chart

def gather():
    nodes, merged, events, leaders = [], {}, {}, []
    for p in NODES:
        try:
            h = http_get(f"http://localhost:{p}/health")
            for r in http_get(f"http://localhost:{p}/data"): merged[f"{r['station']}:{r['seq']}"] = r
            for e in http_get(f"http://localhost:{p}/events"): events[e["id"]] = e
            nodes.append({"port": p, "up": True, "count": h["count"], "clock": h["clock"], "leader": h["leader"]})
            leaders.append(h["leader"])
        except Exception:
            nodes.append({"port": p, "up": False, "count": 0, "clock": 0, "leader": None})
    for n in nodes: n["missing"] = (len(merged) - n["count"]) if n["up"] else None
    up_ports = {n["port"] for n in nodes if n["up"]}
    votes = [l for l in leaders if l in up_ports]
    leader = max(set(votes), key=votes.count) if votes else None
    return nodes, merged, events, leader

def trend(vals):
    if len(vals) < 10: return "►"
    d = sum(vals[-5:]) / 5 - sum(vals[-10:-5]) / 5
    return "▲" if d > 0.3 else ("▼" if d < -0.3 else "►")

def collect():
    nodes, merged, events, leader = gather()
    by = {}
    for r in merged.values(): by.setdefault(r["station"], []).append(r)
    now, stations, history = time.time(), [], {"temp": {}, "humidity": {}, "pressure": {}}
    for st in sorted(by):
        rs = sorted(by[st], key=lambda r: r["ts"]); last = rs[-1]; temps = [r["temp"] for r in rs]
        for m in history: history[m][st] = [r[m] for r in rs[-HISTORY:]]
        stations.append({**last, "online": now - last["ts"] <= OFFLINE_AFTER, "tmin": min(temps), "tmax": max(temps),
                         "tavg": round(sum(temps) / len(temps), 2), "trend": trend(temps)})
    live = [s for s in stations if s["online"]]
    alerts = [f"{s['station']}: {s['temp']}°C exceeds {ALERT_TEMP}°C" for s in stations if s["online"] and s["temp"] > ALERT_TEMP]
    # event log: sort, collapse identical messages detected by several nodes within 8s
    log, last_ts = [], {}
    for e in sorted(events.values(), key=lambda e: e["ts"]):
        if e["msg"] in last_ts and e["ts"] - last_ts[e["msg"]] < 8: continue
        last_ts[e["msg"]] = e["ts"]; log.append(e)
    missing = [n["missing"] for n in nodes if n["up"]]
    return {"nodes": nodes, "leader": leader, "total": len(merged), "alert_temp": ALERT_TEMP, "alerts": alerts,
            "history": history, "stations": stations, "events": log[-20:][::-1],
            "down": sum(1 for n in nodes if not n["up"]), "max_missing": max(missing) if missing else 0,
            "avg_temp": round(sum(s["temp"] for s in live) / len(live), 2) if live else None,
            "hottest": max(live, key=lambda s: s["temp"])["station"] if live else None,
            "online": len(live), "sensors": len(stations)}

def export_csv():
    _, merged, _, _ = gather()
    lines = ["station,seq,timestamp,temp_c,humidity_pct,pressure_hpa,via_node,lamport_clock"]
    for r in sorted(merged.values(), key=lambda r: r["ts"]):
        lines.append(f"{r['station']},{r['seq']},{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))},"
                     f"{r['temp']},{r['humidity']},{r['pressure']},{r.get('via','')},{r.get('lc','')}")
    return "\n".join(lines).encode()

PAGE = """<!doctype html><meta charset=utf-8><title>Distributed Weather Monitor</title>
<style>
body{font-family:sans-serif;max-width:1000px;margin:24px auto;padding:0 16px;color:#222}
h2{margin-bottom:6px}.n{display:inline-block;padding:8px 14px;margin:4px 4px 4px 0;border-radius:8px;color:#fff;font-size:14px}
.up{background:#2e7d32}.down{background:#c62828}.lead{background:#1565c0;color:#fff;padding:4px 12px;border-radius:14px;font-size:14px}
#alerts{background:#c62828;color:#fff;padding:10px 14px;border-radius:8px;margin:10px 0}
#sync{font-size:14px;margin:6px 0}.ok{color:#2e7d32}.warn{color:#e65100}
table{border-collapse:collapse;width:100%;font-size:14px}td,th{border:1px solid #ccc;padding:6px 8px;text-align:left}
tr.hot td{background:#ffe0e0;font-weight:bold}tr.off td{color:#999;background:#f5f5f5}
.b{padding:2px 8px;border-radius:10px;color:#fff;font-size:12px}.bon{background:#2e7d32}.boff{background:#757575}
.tabs button{padding:6px 14px;margin-right:6px;border:1px solid #1976d2;background:#fff;color:#1976d2;border-radius:6px;cursor:pointer}
.tabs button.on{background:#1976d2;color:#fff}
svg{width:100%;height:280px;border:1px solid #ccc;border-radius:8px;margin-top:10px}.lg span{margin-right:14px}
#log{border:1px solid #ccc;border-radius:8px;max-height:260px;overflow:auto}
#log div{padding:5px 10px;border-left:5px solid #90a4ae;border-bottom:1px solid #eee;font-size:13px}
#log .error{border-left-color:#c62828;background:#fff5f5}#log .warn{border-left-color:#f9a825;background:#fffbe6}
#log .info{border-left-color:#2e7d32}#log time{color:#666;margin-right:8px}a.btn{float:right;font-size:14px}
</style>
<h2>Distributed Weather Data Monitoring</h2>
<div><span class=lead id=lead>Coordinator: ...</span></div>
<div id=nodes style="margin-top:8px"></div><div id=sync></div><div id=alerts style="display:none"></div>
<p id=agg></p><table id=t></table>
<h3>History <span class=tabs id=tabs></span></h3><div class=lg id=lg></div><svg id=c viewBox="0 0 900 280"></svg>
<h3>Cluster event log <a class=btn href="/export.csv">&#11015; Export all data (CSV)</a></h3><div id=log></div>
<script>
const COL=['#1976d2','#e64a19','#388e3c','#7b1fa2','#f9a825','#00838f','#6d4c41','#455a64'];
const M={temp:['Temperature','°C'],humidity:['Humidity','%'],pressure:['Pressure','hPa']};
let metric='temp', last=null;
tabs.innerHTML=Object.keys(M).map(k=>`<button id=tab_${k} onclick="setM('${k}')">${M[k][0]}</button>`).join('');
function setM(m){metric=m;if(last)chart(last)}
function chart(d){
 Object.keys(M).forEach(k=>document.getElementById('tab_'+k).className=k==metric?'on':'');
 const h=d.history[metric], names=Object.keys(h).sort(), thr=metric=='temp'?d.alert_temp:null;
 const all=[].concat(...names.map(n=>h[n])); if(thr!==null)all.push(thr);
 if(!all.length){c.innerHTML='';return}
 const N=Math.max(2,...names.map(n=>h[n].length)), lo=Math.min(...all)-1, hi=Math.max(...all)+1;
 const X=i=>50+i*(840/(N-1)), Y=v=>260-(v-lo)/(hi-lo)*240, u=M[metric][1]; let s='';
 for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4;s+=`<line x1=50 x2=890 y1=${Y(v)} y2=${Y(v)} stroke="#eee"/><text x=2 y=${Y(v)+4} font-size=11>${v.toFixed(1)}${u}</text>`}
 if(thr!==null)s+=`<line x1=50 x2=890 y1=${Y(thr)} y2=${Y(thr)} stroke="#c62828" stroke-dasharray="6 4"/><text x=700 y=${Y(thr)-4} font-size=11 fill="#c62828">alert ${thr}°C</text>`;
 names.forEach((n,i)=>{s+=`<polyline fill=none stroke="${COL[i%8]}" stroke-width=2 points="${h[n].map((v,j)=>X(j)+','+Y(v)).join(' ')}"/>`});
 c.innerHTML=s; lg.innerHTML=names.map((n,i)=>`<span style="color:${COL[i%8]}">&#9632; ${n}</span>`).join('');
}
async function tick(){
 const d=await (await fetch('/api')).json(); last=d;
 lead.innerHTML=d.leader?`&#9733; Coordinator: Node ${d.leader}`:'Coordinator: electing...';
 nodes.innerHTML=d.nodes.map(n=>`<span class="n ${n.up?'up':'down'}">${n.leader==null?'':''}${n.port==d.leader?'&#9733; ':''}Node ${n.port}: ${n.up?'UP':'DOWN'} | ${n.count} records | clock ${n.clock}</span>`).join('');
 const ds=d.down?` &nbsp;(${d.down} node${d.down>1?'s':''} down)`:'';
 sync.innerHTML=d.max_missing>0?`<span class=warn>&#8635; Syncing &mdash; up to ${d.max_missing} records still replicating${ds}</span>`:`<span class=ok>&#10004; All live nodes in sync${ds}</span>`;
 alerts.style.display=d.alerts.length?'block':'none'; alerts.innerHTML='&#9888; HEAT ALERT &mdash; '+d.alerts.join(' | ');
 agg.innerHTML=`Unique readings: <b>${d.total}</b> &nbsp; Sensors online: <b>${d.online}/${d.sensors}</b> &nbsp; Avg temp: <b>${d.avg_temp??'-'}°C</b> &nbsp; Hottest: <b>${d.hottest??'-'}</b>`;
 t.innerHTML='<tr><th>Station</th><th>Status</th><th>Via node</th><th>Temp °C</th><th>Humidity %</th><th>Pressure hPa</th><th>Temp min / avg / max</th><th>Trend</th><th>Updated</th></tr>'+
  d.stations.map(r=>`<tr class="${!r.online?'off':(r.temp>d.alert_temp?'hot':'')}"><td>${r.station}</td><td><span class="b ${r.online?'bon':'boff'}">${r.online?'ONLINE':'OFFLINE'}</span></td><td>${r.via??'-'}</td><td>${r.temp}</td><td>${r.humidity}</td><td>${r.pressure}</td><td>${r.tmin} / ${r.tavg} / ${r.tmax}</td><td>${r.trend}</td><td>${new Date(r.ts*1000).toLocaleTimeString()}</td></tr>`).join('');
 log.innerHTML=d.events.map(e=>`<div class="${e.level}"><time>${new Date(e.ts*1000).toLocaleTimeString()}</time>${e.msg}</div>`).join('')||'<div>No events yet</div>';
 chart(d);
}
tick();setInterval(tick,2000);
</script>"""

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        extra = []
        if self.path == "/api": body, ctype = json.dumps(collect()).encode(), "application/json"
        elif self.path == "/export.csv":
            body, ctype = export_csv(), "text/csv"; extra = [("Content-Disposition", "attachment; filename=weather_data.csv")]
        else: body, ctype = PAGE.encode(), "text/html; charset=utf-8"
        self.send_response(200); self.send_header("Content-Type", ctype)
        for k, v in extra: self.send_header(k, v)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

if __name__ == "__main__":
    print("Dashboard on http://localhost:8000")
    ThreadingHTTPServer(("0.0.0.0", 8000), H).serve_forever()