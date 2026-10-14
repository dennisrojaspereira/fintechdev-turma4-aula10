#!/usr/bin/env python3
"""Aula 10 - demo de zonas e regioes (estilo AWS), usada pela pagina /zonas do painel10.

Tres bancos PostgreSQL de verdade, cada um fazendo o papel de um lugar:
  sa-east-1a  pg-primary   primary
  sa-east-1b  pg-standby   standby SINCRONO (synchronous_standby_names): toda escrita espera ele
  us-east-1a  pg-replica   replica ASSINCRONA cross-region

Dois cenarios (um por vez; reset entre eles):
  zona    cai a sa-east-1a          -> failover para sa-east-1b  (perda esperada: zero)
  regiao  o enlace entre regioes cai, depois cai sa-east-1 inteira
                                    -> failover para us-east-1   (perda: o atraso da replica)

O resultado de cada cenario fica em lab10/.runtime/zonas-historico.json (sobrevive ao reset)
para a pagina comparar os dois lado a lado.
"""

import json
import threading
import time

import lab10 as L

STATE = "zonas.json"
HISTORY = "zonas-historico.json"
SYNC_NAME = "az_b"
NODES = {  # papel -> (host, container)
    "primary": ("pg-primary", "lab10-pg-primary"),
    "standby": ("pg-standby", "lab10-pg-standby"),
    "replica": ("pg-replica", "lab10-pg-replica"),
}
TARGET = {"zona": "standby", "regiao": "replica"}


# ---------------------------------------------------------------------------
# Console: o que cada passo executa, o que as aplicacoes registram e o trafego
# ---------------------------------------------------------------------------

CONSOLE = []
CON_LOCK = threading.Lock()
SEQ = [0]
APPS = {"payment-api": "lab10-payment-api", "worker": "lab10-payment-worker", "clientes": "lab10-loadgen"}
WATCH = {"since": None, "last_traffic": 0.0, "last_provider": None, "last_app": {}}


def con(kind, text):
    """kind: step (titulo do passo), info, cmd, out, ok, err, app."""
    with CON_LOCK:
        SEQ[0] += 1
        CONSOLE.append({"id": SEQ[0], "t": L.hms(time.time()), "k": kind, "x": text})
        del CONSOLE[:-500]


def con_clear():
    with CON_LOCK:
        CONSOLE.clear()
    WATCH.update(since=time.time(), last_traffic=time.time(), last_provider=None, last_app={})


def run(cmd, why=None, **kw):
    if why:
        con("info", why)
    con("cmd", "$ " + " ".join(cmd))
    rc, out, err = L.sh(cmd, **kw)
    text = (out or "").strip() or (err or "").strip()
    if rc == 0:
        con("ok", text.splitlines()[0][:160] if text else "ok")
    else:
        con("err", "falhou: " + (text.splitlines()[-1][:160] if text else "rc=%d" % rc))
    return rc, out, err


def sql(host, query, why=None):
    if why:
        con("info", why)
    con("cmd", "psql %s> %s" % (host, query))
    try:
        res = L.pay_db(host, query)
    except L.LabError as e:
        con("err", str(e)[:160])
        raise
    con("ok", (res.strip().splitlines() or ["ok"])[-1][:160] or "ok")
    return res


def wait(desc, fn, timeout):
    con("info", "aguardando: " + desc + "...")
    t0 = time.time()
    ok = L.wait_until(desc, fn, timeout)
    con("ok" if ok else "err", ("%s em %.0f s" % (desc, time.time() - t0)) if ok else "tempo esgotado: " + desc)
    return ok


def watch_apps():
    """Chamado a cada atualizacao da pagina: avisos/erros das aplicacoes e uma linha de trafego a cada 10 s."""
    now = time.time()
    since = WATCH["since"] or now
    for name, cont in APPS.items():
        if not L.running(cont):
            continue
        rc, out, err = L.sh(["docker", "logs", "--since", "%.3f" % since, cont], timeout=5)
        for line in (out + err).splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if rec.get("level") not in ("WARN", "ERROR"):
                continue
            msg = rec.get("message", "")
            detail = rec.get("error") or rec.get("client_key") or ""
            key = name + msg
            last = WATCH["last_app"].get(key, 0)
            if now - last < 20:     # a mesma mensagem no maximo a cada 20 s
                continue
            WATCH["last_app"][key] = now
            con("app", "%s [%s] %s%s" % (name, rec["level"], msg, (" · " + str(detail)[:70]) if detail else ""))
    WATCH["since"] = now
    if now - WATCH["last_traffic"] >= 10:
        try:
            prov = len(provider_transfers())
        except L.LabError:
            prov = None
        if prov is not None and WATCH["last_provider"] is not None:
            host = L.routed_host()
            con("info", "trafego: +%d Pix no provider nos ultimos %.0f s · payment-db -> %s" % (
                prov - WATCH["last_provider"], now - WATCH["last_traffic"], host))
        WATCH["last_provider"] = prov
        WATCH["last_traffic"] = now


def load():
    try:
        return json.loads(L.read_runtime(STATE, "{}"))
    except ValueError:
        return {}


def save(**changes):
    d = load()
    d.update(changes)
    L.write_runtime(STATE, json.dumps(d))
    return d


def history():
    try:
        return json.loads(L.read_runtime(HISTORY, "[]"))
    except ValueError:
        return []


def count(role):
    host, cont = NODES[role]
    if not L.running(cont):
        return None
    try:
        return int(L.pay_db(host, "SELECT count(*) FROM payments"))
    except L.LabError:
        return None


def lag(role):
    """Segundos desde a ultima transacao aplicada (so faz sentido enquanto e standby)."""
    host, cont = NODES[role]
    if not L.running(cont):
        return None
    try:
        if L.pay_db(host, "SELECT pg_is_in_recovery()") != "t":
            return None
        return float(L.pay_db(host, "SELECT coalesce(extract(epoch FROM now() - pg_last_xact_replay_timestamp()), 0)"))
    except (L.LabError, ValueError):
        return None


def provider_transfers():
    code, transfers = L.http_json(L.PROVIDER + "/transfers")
    if code != 200:
        raise L.LabError("provider sem resposta")
    return transfers


def lost_in(role):
    host, cont = NODES[role]
    known = set(r["id"] for r in L.query_json(cont, "payment", "payment", "SELECT id::text AS id FROM payments"))
    return [t for t in provider_transfers() if t.get("payment_id") not in known]


def connected(cont):
    c = L.container(cont)
    return bool(c and L.NET in c["networks"])


# ---------------------------------------------------------------------------
# Passos
# ---------------------------------------------------------------------------

def begin(scenario):
    if scenario not in TARGET:
        raise L.LabError("cenario desconhecido: %s" % scenario)
    if load().get("stage"):
        raise L.LabError("ja existe uma demo em andamento. Reinicie o ambiente antes de outro cenario.")
    if not L.running("lab10-pg-primary") or L.pay_db("pg-replica", "SELECT pg_is_in_recovery()") != "t":
        raise L.LabError("o ambiente nao esta no estado inicial. Reinicie o ambiente (reset).")
    con_clear()
    con("step", "Preparando o cenário: " + ("cai uma zona" if scenario == "zona" else "cai a região"))
    con("info", "subindo o banco da sa-east-1b (standby, copia do primary com pg_basebackup)")
    con("cmd", "$ docker compose --profile zonas up -d pg-standby")
    if L.compose("--profile", "zonas", "up", "-d", "pg-standby") != 0:
        con("err", "falhou")
        raise L.LabError("nao consegui subir o standby da sa-east-1b")
    con("ok", "container lab10-pg-standby no ar")
    ok = wait("sa-east-1b e us-east-1 recebendo o WAL (streaming)", lambda: L.pay_db(
        "pg-primary", "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' "
                      "AND application_name IN ('az_b', 'regiao_b')") == "2", 120)
    if not ok:
        raise L.LabError("as duas replicas nao ficaram em streaming. Reinicie o ambiente.")
    sql("pg-primary", "ALTER SYSTEM SET synchronous_standby_names = '%s'; SELECT pg_reload_conf()" % SYNC_NAME,
        why="ligando a replicacao SINCRONA: o primary so confirma um commit depois que a sa-east-1b gravou")
    wait("sa-east-1b como standby sincrono", lambda: L.pay_db(
        "pg-primary", "SELECT sync_state FROM pg_stat_replication WHERE application_name = '%s'" % SYNC_NAME) == "sync", 30)
    sql("pg-primary", "SELECT application_name, sync_state FROM pg_stat_replication",
        why="quem recebe as mudancas do primary (sync = espera, async = nao espera)")
    if not L.running("lab10-loadgen"):
        run(["docker", "start", "lab10-loadgen"], why="ligando os clientes")
    L.remove_runtime("maintenance")
    base = {"provider": len(provider_transfers())}
    for role in NODES:
        base[role] = count(role)
    L.record("zonas_inicio", cenario=scenario)
    save(scenario=scenario, stage="normal", base=base, t0=time.time())
    con("ok", "cenário pronto: clientes pagando, 3 bancos em dia")
    return {}


def degrade():
    d = load()
    if d.get("scenario") != "regiao" or d.get("stage") != "normal":
        raise L.LabError("passo fora de ordem")
    con("step", "Derrubando o enlace entre regiões")
    run(["docker", "network", "disconnect", "-f", L.NET, "lab10-pg-replica"],
        why="desconectando a us-east-1 da rede: ela para de receber o WAL do primary")
    L.record("enlace_regioes_caiu")
    save(stage="enlace", t_enlace=time.time())
    return {}


def crash():
    d = load()
    if d.get("stage") not in ("normal", "enlace"):
        raise L.LabError("passo fora de ordem")
    if d["scenario"] == "zona":
        con("step", "Derrubando a AZ sa-east-1a")
        run(["docker", "kill", "lab10-pg-primary"], why="o datacenter da sa-east-1a perde energia: o primary morre na hora")
        L.record("falha_zona", zona="sa-east-1a")
    else:
        con("step", "Derrubando a região sa-east-1")
        run(["docker", "kill", "lab10-pg-primary", "lab10-pg-standby", "lab10-loadgen"],
            why="a região inteira cai: primary e standby juntos. Os clientes são pausados")
        L.record("falha_regiao", regiao="sa-east-1")
    save(stage="queda", t_queda=time.time())
    con("err", "sem banco para gravar: a Payment API vai responder erro até o failover")
    return {}


def failover():
    d = load()
    if d.get("stage") != "queda":
        raise L.LabError("passo fora de ordem")
    role = TARGET[d["scenario"]]
    host, cont = NODES[role]
    con("step", "Failover para " + ("sa-east-1b" if role == "standby" else "us-east-1"))
    if role == "replica" and not connected(cont):
        run(["docker", "network", "connect", "--alias", host, L.NET, cont], why="religando a us-east-1 na rede")
    last = float(sql(host, "SELECT coalesce(extract(epoch FROM pg_last_xact_replay_timestamp()), 0)",
                     why="até quando o destino tem dados? (última transação aplicada)") or 0)
    con("info", "último dado no destino: %s · queda: %s" % (L.hms(last), L.hms(d["t_queda"])))
    # fencing: o antigo primary sai da rede antes de existir um novo escritor
    if connected("lab10-pg-primary"):
        run(["docker", "network", "disconnect", "-f", L.NET, "lab10-pg-primary"],
            why="FENCING: tirando o antigo primary da rede antes de promover outro")
    sql(host, "SELECT pg_promote(true, 60)", why="promovendo o destino a primary")
    con("cmd", "$ echo %s > lab10/.runtime/payment-db" % host)
    L.write_runtime("payment-db", host)
    con("ok", "roteamento (o \"Route 53\" do lab): payment-db -> " + host)
    run(["docker", "restart", "lab10-payment-worker"], why="reiniciando o worker: ele só lê o roteamento ao iniciar")
    wait("Payment API aceitando escrita (/health/ready)", lambda: L.http_json(L.API + "/health/ready")[0] == 200, 30)
    done = time.time()
    lost = lost_in(role)
    paid = [t for t in lost if t["status"] == "COMPLETED"]
    janela = max(d["t_queda"] - last, 0) if lost else 0
    result = {"destino": role, "rto_s": round(done - d["t_queda"]), "rto": L.mmss(done - d["t_queda"]),
              "janela": L.mmss(janela), "perdidos": len(lost), "pagos": len(paid),
              "valor": round(sum(float(t["amount"]) for t in paid), 2),
              "exemplos": [{"hora": L.hms(t["created_at"]), "valor": t["amount"], "de": t["payer"], "para": t["payee"]}
                           for t in paid[:3]]}
    con("info", "comparando o provider com %s: o que o provider pagou e o banco novo não tem" % host)
    con("err" if lost else "ok", "%d Pix sem registro no banco novo (%d já pagos, R$ %.2f) · fora do ar %s" % (
        len(lost), len(paid), result["valor"], result["rto"]))
    L.record("zonas_failover", banco=host, **{k: v for k, v in result.items() if k != "exemplos"})
    save(stage="promovido", t_failover=done, failover=result)
    return {"failover": result}


def reconcile():
    d = load()
    if d.get("stage") != "promovido":
        raise L.LabError("passo fora de ordem")
    host, cont = NODES[TARGET[d["scenario"]]]
    con("step", "Reconciliando com o provider")
    con("info", "lendo todas as transferências do provider (GET /transfers) e comparando com " + host)
    before = len(lost_in(TARGET[d["scenario"]]))
    stmts = L.apply_sql(L.classify(L.load_world()))
    con("info", "%d Pix faltando · %d comandos SQL seguros (INSERT com o mesmo id; nada vai ao provider)" % (before, len(stmts)))
    if stmts:
        con("cmd", "psql %s> BEGIN; INSERT INTO payments ... (%d comandos) COMMIT;" % (host, len(stmts)))
        L.psql(cont, "payment", "payment", "BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;", timeout=60)
        con("ok", "aplicado em uma transação")
    time.sleep(2)
    after = len(lost_in(TARGET[d["scenario"]]))
    con("ok" if after == 0 else "err", "Pix sem registro agora: %d" % after)
    if not L.running("lab10-loadgen"):
        run(["docker", "start", "lab10-loadgen"], why="liberando os clientes de novo")
    L.record("reconciliacao", aplicada=True, comandos=len(stmts))
    result = {"antes": before, "depois": after}
    f = d["failover"]
    h = [x for x in history() if x.get("cenario") != d["scenario"]]
    h.append({"cenario": d["scenario"], "rto": f["rto"], "janela": f["janela"], "perdidos": f["perdidos"],
              "valor": f["valor"], "reconstruidos": before - after, "quando": L.hms(time.time())})
    L.write_runtime(HISTORY, json.dumps(h))
    save(stage="reconciliado", reconciliacao=result)
    return {"reconciliacao": result}


# ---------------------------------------------------------------------------
# Fencing (slides 16 e 17): o antigo primary volta. Quem ainda consegue escrever nele?
# ---------------------------------------------------------------------------

# Um "job legado": servico com a configuracao antiga, que ainda aponta para pg-primary.
# Roda de dentro do container da API, entao usa a mesma rede que os servicos usam.
# synchronous_commit=local: o antigo primary ainda exige o standby sincrono, que agora e outro
# banco (ou nao existe mais); sem isso o commit ficaria esperando para sempre.
LEGACY_JOB = r"""
import sys, uuid, psycopg
try:
    c = psycopg.connect(host="pg-primary", dbname="payment", user="payment", password="payment", connect_timeout=3)
    c.execute("SET synchronous_commit = local")
    c.execute("INSERT INTO payments (id, client_key, payer, payee, amount, status, origin) "
              "VALUES (%s, %s, 'ACC-1001', 'ACC-2001', 99.90, 'COMPLETED', 'job-legado')",
              (str(uuid.uuid4()), "legado-" + uuid.uuid4().hex[:8]))
    c.commit()
    print("OK")
except Exception as e:
    print("RECUSADA: " + str(e).strip().splitlines()[0][:120])
"""


def legacy_write():
    con("cmd", "job-legado (na rede da payment-api)> INSERT INTO payments ... em host=pg-primary")
    rc, out, err = L.sh(["docker", "exec", "-i", "lab10-payment-api", "python", "-"], input_text=LEGACY_JOB, timeout=20)
    msg = (out or err).strip().splitlines()[-1] if (out or err).strip() else "sem resposta"
    con("err" if msg == "OK" else "ok", "escrita ACEITA pelo antigo primary (dois escritores!)" if msg == "OK" else msg)
    return {"ok": msg == "OK", "msg": "escrita aceita pelo antigo primary" if msg == "OK" else msg, "hora": L.hms(time.time())}


def wrong_writes():
    try:
        return int(L.pay_db("pg-primary", "SELECT count(*) FROM payments WHERE origin = 'job-legado'"))
    except L.LabError:
        return None


def primary_back():
    d = load()
    if d.get("stage") != "reconciliado":
        raise L.LabError("passo fora de ordem")
    con("step", "O antigo primary volta a ligar")
    run(["docker", "start", "lab10-pg-primary"], why="a energia voltou: o antigo primary liga e ainda se acha primary")
    wait("antigo primary de pé", lambda: L.sh(
        ["docker", "exec", "lab10-pg-primary", "pg_isready", "-U", "payment", "-d", "payment"])[0] == 0, 40)
    L.record("primary_voltou", isolado=not connected("lab10-pg-primary"))
    tentativa = legacy_write()
    fencing = {"isolado": tentativa}
    save(stage="voltou", fencing=fencing)
    return {"fencing": fencing}


def unfence():
    d = load()
    if d.get("stage") != "voltou":
        raise L.LabError("passo fora de ordem")
    con("step", "Sem fencing: religando o antigo primary na rede")
    run(["docker", "network", "connect", "--alias", "pg-primary", L.NET, "lab10-pg-primary"],
        why="desfazendo o fencing: o antigo primary volta a ser alcançável")
    time.sleep(1)
    L.record("fencing_desfeito")
    fencing = dict(d.get("fencing") or {})
    fencing["sem"] = [legacy_write() for _ in range(3)]
    fencing["banco_errado"] = wrong_writes()
    con("err", "%d escritas no banco errado: split-brain" % (fencing["banco_errado"] or 0))
    save(stage="splitbrain", fencing=fencing)
    return {"fencing": fencing}


def refence():
    d = load()
    if d.get("stage") != "splitbrain":
        raise L.LabError("passo fora de ordem")
    con("step", "Refazendo o fencing")
    run(["docker", "network", "disconnect", "-f", L.NET, "lab10-pg-primary"], why="FENCING: o antigo primary sai da rede de novo")
    L.record("fencing", alvo="primary", estado_container="running")
    fencing = dict(d.get("fencing") or {})
    fencing["depois"] = legacy_write()
    fencing["banco_errado"] = wrong_writes()
    save(stage="refencing", fencing=fencing)
    return {"fencing": fencing}


STEPS = {"zona": lambda: begin("zona"), "regiao": lambda: begin("regiao"), "degrade": degrade,
         "crash": crash, "failover": failover, "reconcile": reconcile,
         "volta": primary_back, "semfencing": unfence, "refencing": refence}


def snapshot():
    d = load()
    if d.get("stage"):
        try:
            watch_apps()
        except L.LabError:
            pass
    out = {"stage": d.get("stage"), "scenario": d.get("scenario"), "historico": history(),
           "failover": d.get("failover"), "reconciliacao": d.get("reconciliacao"), "fencing": d.get("fencing"),
           "now": time.time()}
    with CON_LOCK:
        out["console"] = list(CONSOLE)
    for k in ("t_queda", "t_failover", "t_enlace"):
        out[k] = d.get(k)
    base = d.get("base")
    prov = len(provider_transfers())
    out["provider"] = prov - base["provider"] if base else None
    nodes = {}
    for role, (host, cont) in NODES.items():
        c = L.container(cont)
        n = count(role)
        state = "down" if not c or c["status"] != "running" else "up"
        try:
            primary = state == "up" and L.pay_db(host, "SELECT pg_is_in_recovery()") == "f"
        except L.LabError:
            primary = False
        nodes[role] = {"state": state, "net": bool(c and L.NET in c["networks"]), "primary": primary,
                       "count": None if n is None or not base or base.get(role) is None else n - base[role],
                       "lag": lag(role)}
    try:
        nodes["primary"]["sync"] = L.pay_db(
            "pg-primary", "SELECT coalesce(string_agg(application_name || ':' || sync_state, ','), '') "
                          "FROM pg_stat_replication") if nodes["primary"]["state"] == "up" else ""
    except L.LabError:
        nodes["primary"]["sync"] = ""
    out["nodes"] = nodes
    out["route"] = L.routed_host()
    out["api_ready"] = L.http_json(L.API + "/health/ready", timeout=2)[0] == 200
    out["clientes"] = L.running("lab10-loadgen")
    return out
