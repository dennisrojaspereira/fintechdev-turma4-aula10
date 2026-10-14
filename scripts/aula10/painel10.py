#!/usr/bin/env python3
# Painel da Aula 10: uma pagina local para operar o Game Day sem terminal.
#   python scripts/aula10/painel10.py   ->  http://localhost:8098  (demo do professor: /demo)
# Cada botao executa "lab10.py <comando>" (sem shell). So aceita conexoes de 127.0.0.1
# e so executa os comandos da lista abaixo. O cenario do instrutor (chaos start) roda
# em segundo plano e tem o seu proprio quadro, enquanto a equipe opera pelos outros botoes.
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
LAB10 = os.path.join(HERE, "lab10.py")
REPORTS = os.path.join(ROOT, "relatorios")
ROTEIRO = os.path.join(ROOT, "docs", "labs", "24-game-day.md")
DEMO_PAGE = os.path.join(HERE, "demo10.html")
ZONAS_PAGE = os.path.join(HERE, "zonas10.html")
PORT = 8098
ENV = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}

SERVICES = ["primary", "replica", "ledger-db", "kafka", "provider", "api", "worker", "ledger", "loadgen"]
SQL_TARGETS = ["payment", "primary", "replica", "ledger"]
NAME = re.compile(r"[\w .@-]{1,60}")
EVENT = re.compile(r"[a-z0-9][a-z0-9-]{0,40}")


class Bad(Exception):
    pass


def num(v, lo, hi):
    if not re.fullmatch(r"\d{1,5}", str(v)) or not lo <= int(v) <= hi:
        raise Bad("numero fora do intervalo %d-%d: %r" % (lo, hi, v))
    return str(int(v))


def name(v, what):
    v = str(v or "").strip()
    if not NAME.fullmatch(v):
        raise Bad("%s invalido (letras, numeros, espaco, . @ -)" % what)
    return v


def build_argv(cmd, a):
    """Traduz o pedido da pagina em argumentos do lab10.py; recusa o que nao conhece."""
    if cmd in ("up", "down", "reset", "status", "check", "fence"):
        return [cmd]
    if cmd == "reconcile":
        return ["reconcile", "--apply"] if a.get("apply") else ["reconcile"]
    if cmd == "chaos-start":
        return ["chaos", "start", "--lag", num(a.get("lag", 90), 5, 3600), "--volta", num(a.get("volta", 480), 0, 7200)]
    if cmd == "chaos-primary-back":
        return ["chaos", "primary-back"]
    if cmd == "mark":
        ev = str(a.get("evento", "")).strip()
        if not EVENT.fullmatch(ev):
            raise Bad("evento invalido (ex.: detectado, incidente-declarado)")
        nota = str(a.get("nota", "")).strip()[:200]
        return ["mark", ev] + ([nota] if nota else [])
    if cmd == "maintenance":
        if a.get("estado") not in ("on", "off"):
            raise Bad("estado deve ser on ou off")
        return ["maintenance", a["estado"]]
    if cmd == "promote":
        return ["promote", "--autorizado-por", name(a.get("nome"), "nome de quem autorizou")]
    if cmd == "route":
        if a.get("alvo") not in ("primary", "replica"):
            raise Bad("alvo deve ser primary ou replica")
        return ["route", a["alvo"]]
    if cmd in ("start", "restart"):
        if a.get("servico") not in SERVICES:
            raise Bad("servico desconhecido")
        return [cmd, a["servico"]]
    if cmd == "logs":
        if a.get("servico") not in SERVICES:
            raise Bad("servico desconhecido")
        return ["logs", a["servico"], num(a.get("n", 40), 1, 2000)]
    if cmd == "provider":
        chave = str(a.get("chave", "")).strip()
        if chave and not re.fullmatch(r"[\w:.-]{1,120}", chave):
            raise Bad("chave invalida")
        return ["provider"] + ([chave] if chave else [])
    if cmd == "sql":
        if a.get("alvo") not in SQL_TARGETS:
            raise Bad("banco desconhecido")
        q = str(a.get("query", "")).strip()
        if not q:
            raise Bad("consulta vazia")
        return ["sql", a["alvo"], q]
    if cmd == "report":
        argv = ["report", "--equipe", name(a.get("equipe"), "nome da equipe")]
        if a.get("meta"):
            argv += ["--meta-rto", num(a["meta"], 1, 600)]
        return argv
    raise Bad("comando desconhecido: %s" % cmd)


# ---------------------------------------------------------------------------
# Jobs: cada execucao vira um processo cuja saida fica em memoria; a pagina
# busca os pedacos novos por polling (funciona em qualquer navegador e proxy).
# ---------------------------------------------------------------------------

JOBS = {}
LOCK = threading.Lock()
SEQ = [0]


def start_job(argv, kind):
    with LOCK:
        if kind == "cenario" and any(j["kind"] == "cenario" and j["rc"] is None for j in JOBS.values()):
            raise Bad("ja existe um cenario rodando; pare-o antes de iniciar outro")
        SEQ[0] += 1
        jid = str(SEQ[0])
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    proc = subprocess.Popen([sys.executable, "-u", LAB10] + argv, cwd=ROOT, env=ENV,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, creationflags=flags)
    job = {"id": jid, "kind": kind, "argv": argv, "out": "$ lab10 %s\n\n" % " ".join(
        ('"%s"' % x if " " in x else x) for x in argv), "rc": None, "proc": proc, "started": time.time()}
    JOBS[jid] = job

    def pump():
        for line in proc.stdout:
            job["out"] += line.decode("utf-8", errors="replace")
        job["rc"] = proc.wait()
        job["out"] += "\n[exit %d]\n" % job["rc"]
    threading.Thread(target=pump, daemon=True).start()
    # guarda so os ultimos jobs
    with LOCK:
        for old in sorted(JOBS, key=int)[:-40]:
            if JOBS[old]["rc"] is not None:
                del JOBS[old]
    return job


def stop_job(job):
    if job["rc"] is not None:
        return
    try:
        if os.name == "nt":
            job["proc"].send_signal(signal.CTRL_BREAK_EVENT)
        else:
            job["proc"].send_signal(signal.SIGINT)  # o lab10 trata Ctrl+C e cancela o cenario
    except OSError:
        job["proc"].kill()


def quick(argv, timeout=20):
    try:
        r = subprocess.run([sys.executable, LAB10] + argv, cwd=ROOT, env=ENV, capture_output=True, timeout=timeout)
        return (r.stdout + r.stderr).decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        return "(status demorou mais de %ds)" % timeout


def list_reports():
    if not os.path.isdir(REPORTS):
        return []
    files = [f for f in os.listdir(REPORTS) if f.endswith(".md")]
    return sorted(files, key=lambda f: os.path.getmtime(os.path.join(REPORTS, f)), reverse=True)


PAGE = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Painel Game Day</title>
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --surface2: #1c2129; --border: #30363d;
    --text: #e6edf3; --muted: #8b949e; --accent: #2f81f7; --green: #3fb950;
    --red: #f85149; --amber: #d29922;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: var(--bg); color: var(--text); font-family: "Segoe UI", system-ui, sans-serif; }
  header { padding: 1rem 1.5rem; border-bottom: 1px solid var(--border); background: var(--surface);
           display: flex; align-items: baseline; gap: 1rem; flex-wrap: wrap; }
  header h1 { font-size: 1.25rem; }
  header span { color: var(--muted); font-size: .88rem; }
  header nav { margin-left: auto; display: flex; gap: 1.1rem; flex-wrap: wrap; }
  a { color: var(--accent); text-decoration: none; font-size: .88rem; }
  a:hover { text-decoration: underline; }
  .layout { display: grid; grid-template-columns: 380px 1fr; height: calc(100vh - 58px); }
  .menu { overflow-y: auto; border-right: 1px solid var(--border); padding: .9rem 1rem 3rem; }
  .menu h2 { font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; color: var(--muted);
             margin: 1.2rem 0 .45rem; display: flex; gap: .45rem; align-items: center; }
  .menu h2:first-child { margin-top: 0; }
  .menu h2 i { font-style: normal; background: var(--surface2); border: 1px solid var(--border);
               border-radius: 999px; width: 1.35rem; height: 1.35rem; display: inline-grid;
               place-items: center; font-size: .7rem; color: var(--text); }
  .row { display: flex; gap: .4rem; margin: .3rem 0; align-items: center; flex-wrap: wrap; }
  .row > * { min-width: 0; }
  button { padding: .45rem .7rem; background: var(--surface); color: var(--text); border: 1px solid var(--border);
           border-radius: 6px; cursor: pointer; font-size: .86rem; font-family: inherit; }
  button:hover { border-color: var(--accent); }
  button:disabled { opacity: .45; cursor: wait; }
  button.grow { flex: 1; text-align: left; }
  button.danger { border-color: #5a2a2a; }
  button.danger:hover { border-color: var(--red); }
  button.armed { background: #3a1616; border-color: var(--red); color: #ffb3ad; }
  button.primary { border-color: #1f4f8f; }
  input, select { padding: .42rem .5rem; background: var(--bg); color: var(--text);
                  border: 1px solid var(--border); border-radius: 6px; font-size: .86rem; font-family: inherit; }
  input:focus, select:focus { outline: none; border-color: var(--accent); }
  input.w-num { width: 4.6rem; }
  input.flex, select.flex { flex: 1; }
  textarea { width: 100%; min-height: 4.2rem; padding: .45rem .5rem; background: var(--bg); color: var(--text);
             border: 1px solid var(--border); border-radius: 6px; font: .8rem Consolas, Menlo, monospace; resize: vertical; }
  label.small { font-size: .75rem; color: var(--muted); }
  p.hint { font-size: .76rem; color: var(--muted); margin: .15rem 0 .3rem; line-height: 1.35; }
  .reports a { display: block; margin: .2rem 0; font-size: .82rem; }
  .main { display: grid; grid-template-rows: minmax(0, 1.15fr) auto minmax(0, 1fr); min-width: 0; min-height: 0; }
  .pane { display: flex; flex-direction: column; min-height: 0; border-bottom: 1px solid var(--border); }
  .pane-head { padding: .45rem 1.1rem; font-size: .82rem; color: var(--muted); background: var(--surface);
               border-bottom: 1px solid var(--border); display: flex; gap: .8rem; align-items: center; flex-wrap: wrap; }
  .pane-head b { color: var(--text); font-weight: 600; }
  .pane-head .sp { margin-left: auto; display: flex; gap: .6rem; align-items: center; }
  .pane-head button { padding: .2rem .55rem; font-size: .78rem; }
  .run { color: var(--green); } .err { color: var(--red); }
  pre { flex: 1; overflow: auto; padding: .8rem 1.1rem; font: .8rem/1.45 Consolas, Menlo, monospace;
        white-space: pre-wrap; word-break: break-word; }
  pre .pass { color: var(--green); } pre .fail { color: var(--red); } pre .warn { color: var(--amber); }
  pre .cmd { color: var(--accent); }
  #cenario-pane pre { max-height: 22vh; min-height: 5rem; }
  #cenario-pane.idle pre { min-height: 0; max-height: 3.2rem; color: var(--muted); }
  @media (max-width: 860px) {
    .layout { grid-template-columns: 1fr; height: auto; }
    .menu { border-right: 0; border-bottom: 1px solid var(--border); }
    .main { grid-template-rows: auto auto auto; }
    pre { min-height: 35vh; }
    header nav { margin-left: 0; }
  }
</style>
</head>
<body>
<header>
  <h1>Tech Pix · Game Day (Aula 10)</h1>
  <span>cada botão executa o <code>lab10</code>; a saída aparece ao lado</span>
  <nav>
    <a href="/demo">1 · Demo RPO</a>
    <a href="/zonas">2 · Zonas e regiões</a>
    <a href="/roteiro" target="_blank">Roteiro do lab 24</a>
    <a href="http://localhost:8088/health/ready" target="_blank">Payment API :8088</a>
    <a href="http://localhost:8090" target="_blank">Provider :8090</a>
  </nav>
</header>
<div class="layout">
<nav class="menu">

  <h2><i>0</i>Ambiente</h2>
  <div class="row">
    <button class="grow" data-cmd="up">Subir (up)</button>
    <button class="grow" data-cmd="check">Check (invariantes)</button>
  </div>
  <div class="row">
    <button class="grow" data-cmd="down">Derrubar (mantém dados)</button>
    <button class="grow danger" data-cmd="reset" data-confirm>Reset (apaga tudo)</button>
  </div>
  <p class="hint">Antes do Game Day: <b>reset</b> e depois <b>check</b> com os 7 invariantes em PASS.</p>

  <h2><i>1</i>Instrutor: o desastre</h2>
  <div class="row">
    <label class="small">lag (s)</label><input id="lag" class="w-num" value="90">
    <label class="small">volta (s)</label><input id="volta" class="w-num" value="480">
    <button class="danger" id="chaos-btn" data-confirm onclick="startChaos(this)">Chaos start</button>
  </div>
  <div class="row">
    <button class="grow" data-cmd="chaos-primary-back" data-confirm>Religar o antigo primary agora</button>
  </div>
  <p class="hint">Corta a replicação; após <i>lag</i> s derruba primary e Kafka; após <i>volta</i> s religa o antigo primary (split-brain). 0 = não religa.</p>

  <h2><i>2</i>Detectar e declarar</h2>
  <div class="row">
    <input id="pessoa" class="flex" placeholder="seu nome (comandante / quem autoriza)">
  </div>
  <div class="row">
    <button class="grow" onclick="mark('detectado')">mark detectado</button>
    <button class="grow" onclick="mark('incidente-declarado', true)">mark incidente-declarado</button>
  </div>
  <div class="row">
    <input id="ev" class="flex" placeholder="outro marco (ex.: failover-autorizado)">
    <input id="nota" class="flex" placeholder="nota">
    <button onclick="mark(val('ev'), false, val('nota'))">mark</button>
  </div>

  <h2><i>3</i>Conter</h2>
  <div class="row">
    <button class="grow" onclick="run('maintenance', {estado:'on'})">Manutenção ON</button>
    <button class="grow" onclick="run('maintenance', {estado:'off'})">Manutenção OFF (reabrir)</button>
  </div>

  <h2><i>4</i>Recuperar: failover</h2>
  <div class="row">
    <button class="grow danger" data-confirm onclick="promote(this)">Promover a réplica (autorizado por: nome acima)</button>
  </div>
  <div class="row">
    <button class="grow danger" data-cmd="fence" data-confirm>Fence (isolar antigo primary)</button>
  </div>
  <div class="row">
    <button class="grow" onclick="run('route', {alvo:'replica'})">route → replica</button>
    <button class="grow" onclick="run('route', {alvo:'primary'})">route → primary</button>
  </div>
  <div class="row">
    <select id="svc" class="flex">__SERVICES__</select>
    <button onclick="run('start', {servico: val('svc')})">start</button>
    <button onclick="run('restart', {servico: val('svc')})">restart</button>
  </div>

  <h2><i>5</i>Investigar</h2>
  <div class="row">
    <select id="logsvc" class="flex">__SERVICES__</select>
    <input id="logn" class="w-num" value="40">
    <button onclick="run('logs', {servico: val('logsvc'), n: val('logn')})">logs</button>
  </div>
  <div class="row">
    <input id="chave" class="flex" placeholder="chave do cliente (vazio = resumo)">
    <button onclick="run('provider', {chave: val('chave')})">provider</button>
  </div>
  <div class="row">
    <select id="sqlalvo">__SQL__</select>
    <select id="sqlpreset" class="flex" onchange="if(this.value){document.getElementById('sqlq').value=this.value}">
      <option value="">consultas prontas…</option>
      <option value="SELECT status, count(*) FROM payments GROUP BY status">payments por status</option>
      <option value="SELECT id, client_key, status, amount, updated_at FROM payments ORDER BY updated_at DESC LIMIT 15">últimos pagamentos</option>
      <option value="SELECT pg_is_in_recovery()">é réplica? (pg_is_in_recovery)</option>
      <option value="SELECT now() - pg_last_xact_replay_timestamp() AS atraso">atraso da réplica</option>
      <option value="SELECT count(*) FROM outbox WHERE published_at IS NULL">outbox pendente</option>
    </select>
  </div>
  <textarea id="sqlq" placeholder="SELECT ..."></textarea>
  <div class="row"><button class="grow" onclick="run('sql', {alvo: val('sqlalvo'), query: val('sqlq')})">Executar SQL</button></div>

  <h2><i>6</i>Reconciliar e validar</h2>
  <div class="row">
    <button class="grow" onclick="run('reconcile', {})">Reconcile (só mostra)</button>
    <button class="grow danger" data-confirm onclick="run('reconcile', {apply: true}, this)">Reconcile --apply</button>
  </div>
  <div class="row"><button class="grow primary" data-cmd="check">Check (invariantes)</button></div>

  <h2><i>7</i>Relatório</h2>
  <div class="row">
    <input id="equipe" class="flex" placeholder="equipe (ex.: azul)">
    <label class="small">meta RTO (min)</label><input id="meta" class="w-num" value="15">
    <button onclick="run('report', {equipe: val('equipe'), meta: val('meta')}).then(loadReports)">Gerar</button>
  </div>
  <div class="reports" id="reports"></div>
</nav>

<div class="main">
  <section class="pane">
    <div class="pane-head"><b>Status do plantão</b><span id="st-time"></span>
      <span class="sp"><label class="small"><input type="checkbox" id="auto" checked> atualizar a cada 5 s</label>
      <button onclick="refreshStatus()">atualizar</button></span></div>
    <pre id="status">carregando…</pre>
  </section>
  <section class="pane idle" id="cenario-pane">
    <div class="pane-head"><b>Cenário do instrutor</b><span id="cen-state">nenhum cenário rodando</span>
      <span class="sp"><button id="cen-stop" onclick="stopChaos()" disabled>parar cenário (Ctrl+C)</button></span></div>
    <pre id="cenario">Use "Chaos start" para iniciar o desastre. A equipe continua operando pelos outros botões.</pre>
  </section>
  <section class="pane" style="border-bottom:0">
    <div class="pane-head"><b id="label">Saída do comando</b><span id="state"></span>
      <span class="sp"><button onclick="document.getElementById('out').textContent=''">limpar</button></span></div>
    <pre id="out">Clique em "Check (invariantes)" para começar.</pre>
  </section>
</div>
</div>
<script>
const $ = id => document.getElementById(id);
const val = id => $(id).value.trim();
let busy = false;

// lembra nome / equipe neste navegador
for (const id of ['pessoa', 'equipe']) {
  try { const v = localStorage.getItem('gd-' + id); if (v) $(id).value = v; } catch (e) {}
  $(id).addEventListener('change', () => { try { localStorage.setItem('gd-' + id, $(id).value); } catch (e) {} });
}

function esc(s) { return s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function paint(text) {
  return esc(text)
    .replace(/^\$ .*$/gm, m => '<span class="cmd">' + m + '</span>')
    .replace(/\[PASS\]|TODOS OS INVARIANTES OK|\bSIM\b/g, m => '<span class="pass">' + m + '</span>')
    .replace(/\[FAIL\]|AINDA NAO[^\n]*|^erro:.*$|sem resposta[^\n]*|\bexited\b|\bNAO\b/gm, m => '<span class="fail">' + m + '</span>')
    .replace(/\[WARN\]|\[INFO\]|A REGIAO PRINCIPAL ESTA FALHANDO/g, m => '<span class="warn">' + m + '</span>');
}

// botoes destrutivos: o primeiro clique arma, o segundo (em 4 s) executa
function armed(btn) {
  if (!btn || !btn.hasAttribute('data-confirm')) return true;
  if (btn.classList.contains('armed')) { btn.classList.remove('armed'); btn.textContent = btn.dataset.orig; return true; }
  btn.dataset.orig = btn.textContent;
  btn.classList.add('armed');
  btn.textContent = 'clique de novo para confirmar';
  setTimeout(() => { if (btn.classList.contains('armed')) { btn.classList.remove('armed'); btn.textContent = btn.dataset.orig; } }, 4000);
  return false;
}

async function post(path, body) {
  const r = await fetch(path, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body || {}) });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || r.statusText);
  return j;
}

async function follow(id, pre, onDone) {
  let from = 0;
  for (;;) {
    const r = await fetch('/api/job/' + id + '?from=' + from);
    const j = await r.json();
    if (j.text) {
      const atBottom = pre.scrollHeight - pre.scrollTop - pre.clientHeight < 40;
      pre.dataset.raw = (pre.dataset.raw || '') + j.text;
      pre.innerHTML = paint(pre.dataset.raw);
      if (atBottom) pre.scrollTop = pre.scrollHeight;
    }
    from = j.next;
    if (j.rc !== null) { onDone && onDone(j.rc); return j.rc; }
    await new Promise(res => setTimeout(res, 400));
  }
}

async function run(cmd, args, btn) {
  if (btn && !armed(btn)) return;
  if (busy) return;
  busy = true;
  document.querySelectorAll('.menu button').forEach(b => { if (b.id !== 'cen-stop') b.disabled = true; });
  const out = $('out');
  $('label').textContent = cmd;
  $('state').textContent = 'executando…'; $('state').className = 'run';
  out.dataset.raw = ''; out.textContent = '';
  try {
    const job = await post('/api/run', { cmd, args: args || {} });
    await follow(job.id, out, rc => { $('state').textContent = rc === 0 ? 'concluído' : 'terminou com erro (' + rc + ')';
                                       $('state').className = rc === 0 ? '' : 'err'; });
  } catch (e) {
    out.textContent = 'erro: ' + e.message; $('state').textContent = 'recusado'; $('state').className = 'err';
  }
  busy = false;
  document.querySelectorAll('.menu button').forEach(b => b.disabled = false);
  refreshStatus();
}

document.querySelectorAll('button[data-cmd]').forEach(b => b.addEventListener('click', () => run(b.dataset.cmd, {}, b)));

function mark(evento, comNome, nota) {
  if (!evento) { $('out').textContent = 'erro: informe o nome do marco'; return; }
  run('mark', { evento, nota: nota || (comNome ? val('pessoa') : '') });
}
function promote(btn) {
  if (!val('pessoa')) { $('out').textContent = 'erro: preencha "seu nome" em Detectar e declarar: a promoção registra quem autorizou.'; return; }
  run('promote', { nome: val('pessoa') }, btn);
}

// cenario do instrutor em segundo plano
let cenJob = null;
async function startChaos(btn) {
  if (!armed(btn)) return;
  try {
    const job = await post('/api/run', { cmd: 'chaos-start', args: { lag: val('lag'), volta: val('volta') } });
    watchChaos(job.id);
  } catch (e) { $('cenario').textContent = 'erro: ' + e.message; $('cenario-pane').classList.remove('idle'); }
}
function watchChaos(id) {
  cenJob = id;
  const pre = $('cenario');
  $('cenario-pane').classList.remove('idle');
  pre.dataset.raw = ''; pre.textContent = '';
  $('cen-state').textContent = 'rodando'; $('cen-state').className = 'run';
  $('cen-stop').disabled = false; $('chaos-btn').disabled = true;
  follow(id, pre, rc => {
    $('cen-state').textContent = rc === 0 ? 'cenário concluído' : 'cenário encerrado (' + rc + ')';
    $('cen-state').className = rc === 0 ? '' : 'err';
    $('cen-stop').disabled = true; $('chaos-btn').disabled = false; cenJob = null;
  });
}
async function stopChaos() { if (cenJob) await post('/api/job/' + cenJob + '/stop'); }

async function refreshStatus() {
  try {
    const r = await fetch('/api/status');
    const t = await r.text();
    $('status').innerHTML = paint(t);
    $('st-time').textContent = new Date().toLocaleTimeString('pt-BR');
  } catch (e) { $('status').textContent = 'painel sem resposta: ' + e; }
}
setInterval(() => { if ($('auto').checked && !document.hidden) refreshStatus(); }, 5000);

async function loadReports() {
  const r = await fetch('/api/relatorios');
  const files = await r.json();
  $('reports').innerHTML = files.length
    ? files.map(f => '<a target="_blank" href="/relatorios/' + encodeURIComponent(f) + '">' + esc(f) + '</a>').join('')
    : '<p class="hint">nenhum relatório ainda</p>';
}

(async () => {
  refreshStatus(); loadReports();
  const r = await fetch('/api/cenario'); const j = await r.json();
  if (j.id) watchChaos(j.id);   // reabriu a pagina com o cenario rodando
})();
</script>
</body>
</html>"""


def build_page():
    opts = "".join('<option>%s</option>' % s for s in SERVICES)
    sql = "".join('<option>%s</option>' % s for s in SQL_TARGETS)
    return PAGE.replace("__SERVICES__", opts).replace("__SQL__", sql)


# Demo guiada do professor (/demo): os passos rodam neste processo, um de cada vez.
sys.path.insert(0, HERE)
import demo10  # noqa: E402
import zonas10  # noqa: E402

DEMO_LOCK = threading.Lock()
DEMO_STEPS = {"begin": demo10.begin, "cut": demo10.cut, "crash": demo10.crash,
              "promote": demo10.promote, "reconcile": demo10.reconcile}


def demo_step(name, steps=DEMO_STEPS):
    if not DEMO_LOCK.acquire(blocking=False):
        raise Bad("outro passo da demo ainda esta rodando")
    try:
        result = steps[name]()
        return result if isinstance(result, dict) and name != "begin" else {}
    except demo10.L.LabError as e:
        raise Bad(str(e))
    except Exception as e:  # noqa: BLE001 - a pagina precisa ver o erro, nao uma resposta vazia
        raise Bad("erro inesperado: %s: %s" % (type(e).__name__, e))
    finally:
        DEMO_LOCK.release()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if not any(x in self.path for x in ("/api/job/", "/api/status", "/api/demo/board", "/api/zonas/board")):
            sys.stderr.write("  %s\n" % (fmt % args))

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else (
            body if isinstance(body, str) else json.dumps(body)).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        p = u.path
        if p in ("/", "/index.html"):
            self.send(200, build_page(), "text/html")
        elif p == "/demo":
            with open(DEMO_PAGE, "rb") as fh:
                self.send(200, fh.read(), "text/html")
        elif p in ("/aula10", "/aula10/"):
            self.send_response(302)
            self.send_header("Location", "/demo")
            self.end_headers()
        elif p == "/zonas":
            with open(ZONAS_PAGE, "rb") as fh:
                self.send(200, fh.read(), "text/html")
        elif p == "/api/zonas/board":
            try:
                self.send(200, zonas10.snapshot())
            except demo10.L.LabError as e:
                self.send(200, {"error": str(e)})
        elif p == "/api/demo/board":
            try:
                self.send(200, demo10.snapshot())
            except demo10.L.LabError as e:
                self.send(200, {"error": str(e)})
        elif p == "/api/status":
            self.send(200, quick(["status"]), "text/plain")
        elif p == "/api/cenario":
            run = [j for j in JOBS.values() if j["kind"] == "cenario" and j["rc"] is None]
            self.send(200, {"id": run[-1]["id"] if run else None})
        elif p.startswith("/api/job/"):
            job = JOBS.get(p.rsplit("/", 1)[1])
            if not job:
                return self.send(404, {"error": "job nao encontrado"})
            frm = int((parse_qs(u.query).get("from") or ["0"])[0] or 0)
            rc = job["rc"]  # le antes do texto: se ja terminou, o texto esta completo
            text = job["out"]
            self.send(200, {"text": text[frm:], "next": len(text), "rc": rc})
        elif p == "/api/relatorios":
            self.send(200, list_reports())
        elif p.startswith("/relatorios/"):
            f = os.path.basename(p[len("/relatorios/"):].replace("%20", " "))
            if f not in list_reports():
                return self.send(404, {"error": "relatorio nao encontrado"})
            with open(os.path.join(REPORTS, f), "rb") as fh:
                self.send(200, fh.read(), "text/plain")
        elif p == "/roteiro":
            with open(ROTEIRO, "rb") as fh:
                self.send(200, fh.read(), "text/plain")
        else:
            self.send(404, {"error": "nao encontrado"})

    def do_POST(self):
        p = urlparse(self.path).path
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self.send(400, {"error": "json invalido"})
        if p.startswith("/api/demo/") and p.rsplit("/", 1)[1] in DEMO_STEPS:
            name = p.rsplit("/", 1)[1]
            try:
                result = demo_step(name)
            except Bad as e:
                return self.send(400, {"error": str(e)})
            key = {"promote": "promocao", "reconcile": "reconciliacao"}.get(name)
            self.send(200, {key: result} if key else {})
        elif p.startswith("/api/zonas/") and p.rsplit("/", 1)[1] in zonas10.STEPS:
            try:
                self.send(200, demo_step(p.rsplit("/", 1)[1], zonas10.STEPS))
            except Bad as e:
                self.send(400, {"error": str(e)})
        elif p == "/api/run":
            try:
                cmd = str(body.get("cmd", ""))
                argv = build_argv(cmd, body.get("args") or {})
                job = start_job(argv, "cenario" if cmd == "chaos-start" else "comando")
            except Bad as e:
                return self.send(400, {"error": str(e)})
            self.send(200, {"id": job["id"]})
        elif p.startswith("/api/job/") and p.endswith("/stop"):
            job = JOBS.get(p.split("/")[3])
            if not job:
                return self.send(404, {"error": "job nao encontrado"})
            stop_job(job)
            self.send(200, {"ok": True})
        else:
            self.send(404, {"error": "nao encontrado"})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    print("Painel do Game Day (Aula 10): http://localhost:%d" % PORT)
    print("Demo guiada do professor:     http://localhost:%d/demo" % PORT)
    print("Demo de zonas e regioes:      http://localhost:%d/zonas" % PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for j in list(JOBS.values()):
            stop_job(j)
