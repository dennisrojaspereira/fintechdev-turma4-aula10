#!/usr/bin/env python3
"""Aula 10 - Game Day de Disaster Recovery. Controlador do laboratorio.

Ambiente
  up                         sobe o ambiente (build na primeira vez) e espera a replica sincronizar
  down                       derruba os containers (mantem os dados)
  reset                      apaga TUDO e recria do zero (entre turmas)
  status                     visao do plantao: bancos, replica, roteamento, servicos, filas
  logs <servico> [n]         ultimas linhas de log (primary, replica, kafka, provider, api, worker, ledger, loadgen)

Instrutor
  chaos start [--lag 90] [--volta 480]
                             corta a replicacao; LAG s depois derruba o primary e o Kafka;
                             VOLTA s depois da queda religa o antigo primary (0 = nao religa)
  chaos primary-back         religa o antigo primary agora (o momento do split-brain)

Equipes
  mark <evento> [nota]       registra um marco humano (ex.: detectado, incidente-declarado, failover-autorizado)
  maintenance on|off         suspende/reabre novos pagamentos na Payment API (contencao / reabertura)
  promote --autorizado-por NOME   promove a replica a primary (decisao humana, registrada)
  fence [primary]            isola o antigo primary da rede (retira a autoridade de escrita)
  route primary|replica      troca o roteamento do payment-db
  start <servico>            liga um servico parado (ex.: start kafka)
  restart <servico>          reinicia um servico (ex.: restart worker)
  provider [chave]           o que o provider sabe (resumo, ou uma transferencia)
  sql <payment|primary|replica|ledger> "<consulta>"
  reconcile [--apply]        compara Tech Pix x provider x Ledger; --apply corrige SO o que e seguro
  check                      verifica os invariantes financeiros e operacionais
  report [--equipe NOME] [--meta-rto MIN]   gera o relatorio do Game Day com as medicoes

Requer Docker com "docker compose". Sem dependencias Python externas.
A saida usa apenas ASCII para funcionar em qualquer console Windows.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNTIME = os.path.join(ROOT, "lab10", ".runtime")
STATE_FILE = os.path.join(RUNTIME, "state.json")
REPORTS = os.path.join(ROOT, "relatorios")
COMPOSE_FILE = os.path.join(ROOT, "docker-compose.lab10.yml")
NET = "lab10-net"
API = "http://localhost:8088"
PROVIDER = "http://localhost:8090"

SERVICES = [  # alias, container
    ("primary", "lab10-pg-primary"), ("replica", "lab10-pg-replica"), ("ledger-db", "lab10-pg-ledger"),
    ("kafka", "lab10-kafka"), ("provider", "lab10-provider"), ("api", "lab10-payment-api"),
    ("worker", "lab10-payment-worker"), ("ledger", "lab10-ledger"), ("loadgen", "lab10-loadgen"),
]
ALIAS = dict(SERVICES)
ALIAS.update({"pg-primary": "lab10-pg-primary", "pg-replica": "lab10-pg-replica", "pg-ledger": "lab10-pg-ledger",
              "payment-api": "lab10-payment-api", "payment-worker": "lab10-payment-worker"})
DB_CONTAINER = {"pg-primary": "lab10-pg-primary", "pg-replica": "lab10-pg-replica"}
ROUTES = {"primary": "pg-primary", "replica": "pg-replica"}


class LabError(Exception):
    pass


# ---------------------------------------------------------------------------
# Infra
# ---------------------------------------------------------------------------

def sh(cmd, input_text=None, timeout=120):
    try:
        p = subprocess.run(cmd, input=input_text, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        raise LabError("comando nao encontrado: %s (o Docker esta instalado?)" % cmd[0])
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    return p.returncode, p.stdout, p.stderr


def compose(*args):
    """docker compose (plugin) ou docker-compose (binario antigo), o que existir."""
    for base in (["docker", "compose"], ["docker-compose"]):
        try:
            if subprocess.run(base + ["version"], capture_output=True).returncode == 0:
                return subprocess.call(base + ["-f", COMPOSE_FILE] + list(args))
        except FileNotFoundError:
            continue
    raise LabError("nem 'docker compose' nem 'docker-compose' encontrados")


def container(name):
    rc, out, _ = sh(["docker", "inspect", "-f",
                     "{{.State.Status}}|{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", name])
    if rc != 0:
        return None
    status, _, nets = out.strip().partition("|")
    return {"status": status, "networks": nets.split()}


def running(name):
    c = container(name)
    return bool(c and c["status"] == "running")


def psql(cont, db, user, sql, tuples=True, timeout=30):
    cmd = ["docker", "exec", "-i", cont, "psql", "-U", user, "-d", db, "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    if tuples:
        cmd += ["-A", "-t"]
    rc, out, err = sh(cmd, input_text=sql, timeout=timeout)
    if rc != 0:
        raise LabError((err or out).strip().splitlines()[-1] if (err or out).strip() else "psql falhou em " + cont)
    return out


def query_json(cont, db, user, sql):
    out = psql(cont, db, user, "SELECT coalesce(json_agg(t), '[]'::json) FROM (" + sql + ") t;")
    return json.loads(out.strip() or "[]")


def scalar(cont, db, user, sql):
    return psql(cont, db, user, sql + ";").strip()


def pay_db(host_or_cont, sql):
    cont = DB_CONTAINER.get(host_or_cont, host_or_cont)
    return scalar(cont, "payment", "payment", sql)


def http_json(url, method="GET", timeout=5):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except ValueError:
            return e.code, {}
    except (urllib.error.URLError, OSError, ValueError):
        return 0, {}


def read_runtime(name, default=None):
    try:
        with open(os.path.join(RUNTIME, name), encoding="utf-8") as f:
            return f.read().strip() or default
    except OSError:
        return default


def write_runtime(name, value):
    os.makedirs(RUNTIME, exist_ok=True)
    with open(os.path.join(RUNTIME, name), "w", encoding="utf-8") as f:
        f.write(value + "\n")


def remove_runtime(name):
    try:
        os.remove(os.path.join(RUNTIME, name))
    except OSError:
        pass


def heartbeat(name):
    try:
        with open(os.path.join(RUNTIME, name), encoding="utf-8") as f:
            data = json.load(f)
        data["age"] = time.time() - data.get("ts", 0)
        return data
    except (OSError, ValueError):
        return None


def routed_host():
    return read_runtime("payment-db", "pg-primary")


# ---------------------------------------------------------------------------
# Estado do exercicio (linha do tempo medida)
# ---------------------------------------------------------------------------

def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"created": time.time(), "events": []}


def save_state(state):
    os.makedirs(RUNTIME, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def record(kind, **detail):
    state = load_state()
    state["events"].append(dict(detail, type=kind, ts=time.time()))
    save_state(state)


def events(kind=None):
    evs = load_state()["events"]
    return [e for e in evs if kind is None or e["type"] == kind]


def last_event(kind):
    evs = events(kind)
    return evs[-1] if evs else None


def hms(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "-"


def mmss(seconds):
    if seconds is None:
        return "-"
    seconds = int(round(seconds))
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    return "%s%02d:%02d" % (sign, seconds // 60, seconds % 60)


# ---------------------------------------------------------------------------
# Ambiente
# ---------------------------------------------------------------------------

def wait_until(desc, fn, timeout):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if fn():
                return True
        except LabError:
            pass
        time.sleep(2)
    print("  [!] tempo esgotado esperando: " + desc)
    return False


def cmd_up(_args):
    os.makedirs(RUNTIME, exist_ok=True)
    if read_runtime("payment-db") is None:
        write_runtime("payment-db", "pg-primary")
    write_runtime("loadgen-clients", "6")
    print("Subindo o ambiente do lab 10 (a primeira vez faz o build da imagem; pode levar alguns minutos)...")
    rc = compose("up", "-d", "--build")
    if rc != 0:  # maquina lenta: o Kafka pode demorar mais que o healthcheck na primeira subida
        print("Primeira tentativa falhou; tentando de novo...")
        if compose("up", "-d") != 0:
            raise LabError("docker compose up falhou (veja a saida acima; 'logs kafka' ajuda)")
    print("Esperando a replica sincronizar com o primary...")
    wait_until("replica em streaming", lambda: pay_db(
        "pg-primary", "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'") == "1", 120)
    print("Esperando a Payment API...")
    wait_until("payment-api pronta", lambda: http_json(API + "/health/ready")[0] == 200, 60)

    def healthy(name):
        hb = heartbeat(name)
        return bool(hb and hb["age"] < 10 and hb.get("db_ok") and hb.get("kafka_ok"))
    print("Esperando worker e ledger conectarem no banco e no Kafka...")
    wait_until("worker e ledger saudaveis", lambda: healthy("worker.json") and healthy("ledger.json"), 90)
    if not events():
        record("ambiente_no_ar")
    print()
    cmd_status([])


def cmd_down(_args):
    compose("down")


def cmd_reset(_args):
    print("Apagando o ambiente do lab 10 (containers e volumes)...")
    compose("down", "-v", "--remove-orphans")
    for name in ("state.json", "payment-db", "maintenance", "loadgen-clients"):
        remove_runtime(name)
    save_state({"created": time.time(), "events": []})
    cmd_up([])


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def db_role(cont):
    try:
        return "REPLICA (somente leitura)" if pay_db(cont, "SELECT pg_is_in_recovery()") == "t" else "PRIMARY (aceita escrita)"
    except LabError as e:
        return "sem resposta: " + str(e)


def cmd_status(_args):
    fail = last_event("falha_regiao")
    print("TECH PIX - LAB 10 (Game Day)   %s%s" % (
        hms(time.time()), ("   falha ha " + mmss(time.time() - fail["ts"])) if fail else ""))
    print()
    print("COMPONENTES")
    for alias, name in SERVICES:
        c = container(name)
        st = c["status"] if c else "inexistente"
        extra = []
        if c and NET not in c["networks"]:
            extra.append("ISOLADO DA REDE (fencing)")
        if st == "running" and alias in ("primary", "replica"):
            extra.append(db_role(name))
            if alias == "primary":
                try:
                    extra.append("replicas conectadas: " + pay_db(
                        name, "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"))
                except LabError:
                    pass
            else:
                try:
                    row = pay_db(name, "SELECT coalesce((SELECT status FROM pg_stat_wal_receiver), 'PARADA') || '|' || "
                                       "coalesce(extract(epoch FROM now() - pg_last_xact_replay_timestamp())::int, 0) || '|' || "
                                       "CASE WHEN pg_is_in_recovery() THEN 't' ELSE 'f' END")
                    recv, lag, rec = row.split("|")
                    if rec == "t":
                        extra.append("replicacao: %s" % ("streaming" if recv == "streaming" else "PARADA"))
                        extra.append("ultima transacao aplicada ha %ss" % lag)
                except (LabError, ValueError):
                    pass
        if alias == "api" and st == "running":
            code, info = http_json(API + "/internal/info")
            rcode, ready = http_json(API + "/health/ready")
            if code == 200:
                extra.append("banco: %s" % info.get("db_target"))
                extra.append("manutencao: %s" % ("ON" if info.get("maintenance") else "off"))
            extra.append("ready: %s" % ("SIM" if rcode == 200 else "NAO (%s)" % ready.get("reason", "?")))
        if alias in ("worker", "ledger") and st == "running":
            hb = heartbeat(alias + ".json")
            if not hb or hb["age"] > 10:
                extra.append("sem heartbeat")
            else:
                if alias == "worker":
                    extra.append("banco: %s (lido no boot)" % hb.get("db_target"))
                extra.append("db: %s" % ("ok" if hb.get("db_ok") else "FALHANDO"))
                extra.append("kafka: %s" % ("ok" if hb.get("kafka_ok") else "FALHANDO"))
                if alias == "ledger":
                    extra.append("eventos: %s (repetidos ignorados: %s)" % (hb.get("processed"), hb.get("duplicates_ignored")))
                if hb.get("last_error") and not (hb.get("db_ok") and hb.get("kafka_ok")):
                    extra.append("ultimo erro: " + str(hb["last_error"])[:70])
        if alias == "loadgen" and st == "running":
            extra.append("clientes ativos: " + read_runtime("loadgen-clients", "6"))
        print("  %-9s %-10s %s" % (alias, st, " | ".join(extra)))

    host = routed_host()
    print()
    print("ROTEAMENTO  payment-db -> %s      manutencao: %s" % (
        host, "ON" if read_runtime("maintenance") is not None or os.path.exists(os.path.join(RUNTIME, "maintenance")) else "off"))
    print()
    print("PAGAMENTOS no banco roteado (%s)" % host)
    try:
        rows = query_json(DB_CONTAINER[host], "payment", "payment",
                          "SELECT status, count(*) AS n FROM payments GROUP BY status ORDER BY status")
        pend = pay_db(host, "SELECT count(*) FROM outbox WHERE published_at IS NULL")
        print("  " + "   ".join("%s %s" % (r["status"], r["n"]) for r in rows) + "   | outbox pendente: " + pend)
    except LabError as e:
        print("  sem acesso: " + str(e))
    code, transfers = http_json(PROVIDER + "/transfers")
    print("PROVIDER")
    if code == 200:
        counts = defaultdict(int)
        for t in transfers:
            counts[t["status"]] += 1
        print("  " + "   ".join("%s %s" % kv for kv in sorted(counts.items())) + "   (total %d)" % len(transfers))
    else:
        print("  sem resposta")


# ---------------------------------------------------------------------------
# Instrutor: o desastre
# ---------------------------------------------------------------------------

def cmd_chaos(args):
    sub = args[0] if args else ""
    if sub == "start":
        lag = int(args[args.index("--lag") + 1]) if "--lag" in args else 90
        back = int(args[args.index("--volta") + 1]) if "--volta" in args else 480
        if not running("lab10-pg-primary"):
            raise LabError("o primary nao esta rodando. Rode 'reset' antes de um novo Game Day.")
        pay_db("pg-primary", "ALTER ROLE replicator NOLOGIN; "
                             "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_replication")
        record("replicacao_cortada")
        print("[cenario] %s replicacao cortada. A regiao principal cai em %d s." % (hms(time.time()), lag))
        print("[cenario] deixe este terminal aberto e use OUTRO para operar (Ctrl+C aqui cancela o cenario).")
        countdown(lag)
        write_runtime("loadgen-clients", "12")
        sh(["docker", "kill", "lab10-pg-primary", "lab10-kafka"])
        record("falha_regiao", lag_configurado=lag)
        print()
        print("=" * 64)
        print("  %s - A REGIAO PRINCIPAL ESTA FALHANDO" % hms(time.time()))
        print("  trafego em 2x | Payment sem banco primario | Kafka fora | provider processando")
        print("  Equipes: comecem por 'status' em outro terminal. O relogio esta correndo.")
        print("=" * 64)
        if back > 0:
            time.sleep(back)
            primary_back()

    elif sub == "primary-back":
        primary_back()
    else:
        raise LabError("uso: chaos start [--lag 90] [--volta 480] | chaos primary-back")


def countdown(seconds):
    end = time.time() + seconds
    while time.time() < end:
        left = int(round(end - time.time()))
        if left % 15 == 0 and left > 0:
            print("  ... %d s" % left)
        time.sleep(1)


def primary_back():
    c = container("lab10-pg-primary")
    if not c:
        raise LabError("container do primary nao existe")
    if c["status"] == "running":
        print("[cenario] o antigo primary ja esta ligado.")
        return
    sh(["docker", "start", "lab10-pg-primary"])
    isolated = NET not in c["networks"]
    record("primary_voltou", isolado=isolated)
    print("[cenario] %s o antigo primary voltou a ligar. Isolado da rede (fencing): %s" % (
        hms(time.time()), "SIM" if isolated else "NAO"))


# ---------------------------------------------------------------------------
# Equipes: operacao
# ---------------------------------------------------------------------------

def cmd_mark(args):
    if not args:
        raise LabError("uso: mark <evento> [nota]   ex.: mark detectado | mark failover-autorizado Ana")
    record("marco", evento=args[0], nota=" ".join(args[1:]))
    print("marco registrado: %s %s (%s)" % (args[0], " ".join(args[1:]), hms(time.time())))


def cmd_maintenance(args):
    if not args or args[0] not in ("on", "off"):
        raise LabError("uso: maintenance on|off")
    if args[0] == "on":
        write_runtime("maintenance", "on")
        print("Payment API em manutencao: novos pagamentos recebem 503. Consultas continuam.")
    else:
        remove_runtime("maintenance")
        print("Payment API reaberta para novos pagamentos.")
    record("manutencao", estado=args[0])


def cmd_promote(args):
    who = None
    if "--autorizado-por" in args:
        i = args.index("--autorizado-por")
        who = " ".join(args[i + 1:]).strip()
    if not who:
        raise LabError("promover a replica e uma decisao humana. Uso: promote --autorizado-por NOME")
    if not running("lab10-pg-replica"):
        raise LabError("a replica nao esta rodando")
    if pay_db("pg-replica", "SELECT pg_is_in_recovery()") != "t":
        raise LabError("a replica ja foi promovida (ja e primary)")
    last = pay_db("pg-replica", "SELECT coalesce(extract(epoch FROM pg_last_xact_replay_timestamp()), 0)")
    last_ts = float(last) if last else 0.0
    fail = last_event("falha_regiao")
    print("Ultima transacao aplicada na replica: %s" % hms(last_ts))
    if fail:
        print("Intervalo ate a falha da regiao: %s (escritas desse intervalo NAO estao na replica)" % mmss(fail["ts"] - last_ts))
    pay_db("pg-replica", "SELECT pg_promote(true, 60)")
    prim = container("lab10-pg-primary")
    fenced = bool(prim) and NET not in prim["networks"]
    record("promocao", autorizado_por=who, ultima_transacao_replica=last_ts, primary_isolado=fenced)
    print("Replica promovida a PRIMARY (%s), autorizado por %s." % (hms(time.time()), who))
    if not fenced:
        print("ATENCAO: o antigo primary NAO esta isolado. Se ele voltar, havera dois escritores.")
    print("Lembrete: promover nao muda o roteamento nem quem ja estava conectado no banco antigo.")


def cmd_fence(args):
    target = args[0] if args else "primary"
    name = ALIAS.get(target)
    if name not in DB_CONTAINER.values():
        raise LabError("uso: fence [primary|replica]")
    c = container(name)
    if not c:
        raise LabError(name + " nao existe")
    if NET not in c["networks"]:
        print("%s ja esta isolado da rede." % target)
        return
    rc, _, err = sh(["docker", "network", "disconnect", "-f", NET, name])
    if rc != 0:
        raise LabError("falha no fencing: " + err.strip())
    record("fencing", alvo=target, estado_container=c["status"])
    print("Fencing aplicado: %s desconectado da rede %s (%s)." % (target, NET, hms(time.time())))
    print("Nenhum servico alcanca esse banco, mesmo que ele volte a ligar.")


def cmd_route(args):
    if not args or args[0] not in ROUTES:
        raise LabError("uso: route primary|replica")
    host = ROUTES[args[0]]
    write_runtime("payment-db", host)
    record("roteamento", destino=host)
    print("Roteamento: payment-db -> %s (%s)" % (host, hms(time.time())))
    try:
        if pay_db(host, "SELECT pg_is_in_recovery()") == "t":
            print("ATENCAO: %s ainda e replica (somente leitura). Escritas vao falhar." % host)
    except LabError as e:
        print("ATENCAO: %s nao responde: %s" % (host, e))
    print("A payment-api le o roteamento a cada requisicao. Confira os outros componentes em 'status'.")


def service_cmd(verb, args):
    if not args or args[0] not in ALIAS:
        raise LabError("uso: %s <%s>" % (verb, "|".join(a for a, _ in SERVICES)))
    rc, _, err = sh(["docker", verb, ALIAS[args[0]]])
    if rc != 0:
        raise LabError(err.strip())
    record(verb, servico=args[0])
    print("%s: %s (%s)" % (verb, args[0], hms(time.time())))


def cmd_logs(args):
    if not args or args[0] not in ALIAS:
        raise LabError("uso: logs <servico> [linhas]")
    n = args[1] if len(args) > 1 else "40"
    subprocess.call(["docker", "logs", "--tail", n, ALIAS[args[0]]])


def cmd_provider(args):
    if args:
        code, data = http_json(PROVIDER + "/transfers/" + args[0])
        print(json.dumps(data, indent=2) if code else "provider sem resposta")
        return
    code, transfers = http_json(PROVIDER + "/transfers")
    if code != 200:
        raise LabError("provider sem resposta")
    counts = defaultdict(int)
    for t in transfers:
        counts[t["status"]] += 1
    print("Provider: %d transferencias  %s" % (len(transfers), "  ".join("%s=%s" % kv for kv in sorted(counts.items()))))
    print("Ultimas 10:")
    for t in transfers[-10:]:
        print("  %s  %-9s  %8s  payment_id=%s  client_key=%s" % (
            hms(t["created_at"]), t["status"], t["amount"], t["payment_id"], t["client_key"]))


def cmd_sql(args):
    if len(args) < 2:
        raise LabError('uso: sql <payment|primary|replica|ledger> "SELECT ..."')
    target, query = args[0], " ".join(args[1:])
    if target == "ledger":
        cont, db, user = "lab10-pg-ledger", "ledger", "ledger"
    elif target == "payment":
        cont, db, user = DB_CONTAINER[routed_host()], "payment", "payment"
    elif target in ("primary", "replica"):
        cont, db, user = ALIAS[target], "payment", "payment"
    else:
        raise LabError("alvo invalido: " + target)
    print(psql(cont, db, user, query.rstrip(";") + ";", tuples=False))


# ---------------------------------------------------------------------------
# Reconciliacao e invariantes
# ---------------------------------------------------------------------------

LOCAL_SQL = ("SELECT id::text AS id, client_key, status, amount::text AS amount, payer, payee, provider_ref, origin, "
             "extract(epoch FROM created_at)::float AS created, extract(epoch FROM updated_at)::float AS updated "
             "FROM payments")
LEDGER_SQL = ("SELECT payment_id::text AS payment_id, count(*) AS n, "
              "sum(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END)::text AS debit, "
              "sum(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END)::text AS credit "
              "FROM ledger_entries GROUP BY payment_id")


def load_world():
    # Ordem importa: provider -> ledger -> banco local. Tudo que o provider ou o Ledger ja tinham
    # nasceu antes no banco local; lendo o local por ultimo, um Pix novo nao parece "desconhecido".
    w = {"host": routed_host(), "now": time.time()}
    code, transfers = http_json(PROVIDER + "/transfers")
    if code != 200:
        raise LabError("provider sem resposta: nao da para reconciliar sem a fonte externa")
    w["provider"] = transfers
    w["ledger"] = query_json("lab10-pg-ledger", "ledger", "ledger", LEDGER_SQL)
    cont = DB_CONTAINER[w["host"]]
    try:
        w["local"] = query_json(cont, "payment", "payment", LOCAL_SQL)
        w["read_only"] = pay_db(cont, "SELECT pg_is_in_recovery()") == "t"
        w["outbox_stuck"] = int(pay_db(cont, "SELECT count(*) FROM outbox WHERE published_at IS NULL "
                                             "AND created_at < now() - interval '15 seconds'"))
        w["quarantine"] = set(r["client_key"] for r in query_json(cont, "payment", "payment",
                                                                  "SELECT client_key FROM quarantine"))
        w["local_ok"] = True
    except LabError as e:
        w.update(local=[], read_only=True, outbox_stuck=0, quarantine=set(), local_ok=False, local_error=str(e))
    return w


CATEGORY_HELP = [
    ("DUPLICADO", "mesma chave do cliente liquidada 2+ vezes no provider: dinheiro saiu em dobro",
     "quarentena (estorno exige decisao humana; nada e reenviado)"),
    ("AUSENTE_LOCAL", "o provider tem a transferencia; o banco recuperado nao tem o pagamento",
     "reconstruir o pagamento a partir do provider (mesmo id) e publicar o evento"),
    ("CONFLITO_CHAVE", "chave do cliente aponta para outro pagamento local; o provider liquidou o antigo",
     "quarentena (risco de duplicacao)"),
    ("ESTADO_DESATUALIZADO", "local PROCESSING/UNKNOWN ha mais de 10s, mas o provider ja decidiu",
     "atualizar o estado a partir do provider"),
    ("SEM_REGISTRO_PROVIDER", "local PROCESSING/UNKNOWN ha mais de 20s e o provider nunca recebeu",
     "marcar FAILED (nenhum dinheiro saiu)"),
    ("DIVERGENCIA_GRAVE", "local e provider discordam sobre um pagamento terminado",
     "quarentena + investigacao"),
    ("LEDGER_FALTANDO", "pagamento COMPLETED sem os 2 lancamentos no Ledger",
     "republicar o evento (o Ledger e idempotente)"),
    ("LEDGER_ORFAO", "lancamento no Ledger para pagamento que o banco recuperado nao conhece",
     "nenhuma (some quando o pagamento for reconstruido)"),
]


def classify(w):
    now = w["now"]
    local_by_id = dict((p["id"], p) for p in w["local"])
    local_by_key = dict((p["client_key"], p) for p in w["local"])
    prov_by_pid = dict((t["payment_id"], t) for t in w["provider"] if t.get("payment_id"))
    by_client = defaultdict(list)
    for t in w["provider"]:
        by_client[t.get("client_key")].append(t)
    ledger_by_pid = dict((r["payment_id"], r) for r in w["ledger"])
    cats = defaultdict(list)

    dup_keys = set(ck for ck, ts in by_client.items()
                   if ck and sum(1 for t in ts if t["status"] == "COMPLETED") > 1)
    for ck in sorted(dup_keys):
        cats["DUPLICADO"].append({"client_key": ck, "transfers": by_client[ck]})

    for t in w["provider"]:
        pid, ck = t.get("payment_id"), t.get("client_key")
        if ck in dup_keys:
            continue
        p = local_by_id.get(pid)
        if p is None:
            other = local_by_key.get(ck)
            if other is None:
                cats["AUSENTE_LOCAL"].append({"transfer": t})
            elif t["status"] == "COMPLETED":
                cats["CONFLITO_CHAVE"].append({"client_key": ck, "transfer": t, "local": other})
            continue
        if p["status"] in ("PROCESSING", "UNKNOWN") and t["status"] in ("COMPLETED", "FAILED"):
            if now - p["updated"] > 10:
                cats["ESTADO_DESATUALIZADO"].append({"local": p, "transfer": t})
        elif p["status"] in ("COMPLETED", "FAILED") and t["status"] in ("COMPLETED", "FAILED") \
                and p["status"] != t["status"]:
            cats["DIVERGENCIA_GRAVE"].append({"local": p, "transfer": t})

    for p in w["local"]:
        if p["status"] in ("PROCESSING", "UNKNOWN") and p["id"] not in prov_by_pid and now - p["created"] > 20:
            cats["SEM_REGISTRO_PROVIDER"].append({"local": p})
        if p["status"] == "COMPLETED" and now - p["updated"] > 15:
            led = ledger_by_pid.get(p["id"])
            if not led or int(led["n"]) != 2:
                cats["LEDGER_FALTANDO"].append({"local": p})

    for pid in ledger_by_pid:
        if pid not in local_by_id:
            t = prov_by_pid.get(pid)
            if t and t.get("client_key") in w["quarantine"]:
                continue
            cats["LEDGER_ORFAO"].append({"payment_id": pid})
    return cats


def q(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def outbox_sql(pid, ck, payer, payee, amount, e2e, republish=False):
    payload = json.dumps({"payment_id": pid, "client_key": ck, "payer": payer, "payee": payee,
                          "amount": str(amount), "e2e_id": e2e})
    conflict = "DO UPDATE SET published_at = NULL" if republish else "DO NOTHING"
    return ("INSERT INTO outbox (event_id, event_type, payload) VALUES (%s, 'PaymentCompleted', %s::jsonb) "
            "ON CONFLICT (event_id) %s;" % (q(pid + ":PaymentCompleted"), q(payload), conflict))


def apply_sql(cats):
    stmts = []
    for item in cats["AUSENTE_LOCAL"]:
        t = item["transfer"]
        status = t["status"] if t["status"] in ("COMPLETED", "FAILED") else "PROCESSING"
        stmts.append("INSERT INTO payments (id, client_key, payer, payee, amount, status, provider_ref, origin, "
                     "created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, 'reconciliation', "
                     "to_timestamp(%s), now()) ON CONFLICT DO NOTHING;" % (
                         q(t["payment_id"]), q(t["client_key"]), q(t["payer"]), q(t["payee"]), q(t["amount"]),
                         q(status), q(t["e2e_id"]), t["created_at"]))
        if status == "COMPLETED":
            stmts.append(outbox_sql(t["payment_id"], t["client_key"], t["payer"], t["payee"], t["amount"], t["e2e_id"]))
    for item in cats["ESTADO_DESATUALIZADO"]:
        p, t = item["local"], item["transfer"]
        stmts.append("UPDATE payments SET status = %s, provider_ref = %s, updated_at = now() WHERE id = %s "
                     "AND status IN ('PROCESSING', 'UNKNOWN');" % (q(t["status"]), q(t["e2e_id"]), q(p["id"])))
        if t["status"] == "COMPLETED":
            stmts.append(outbox_sql(p["id"], p["client_key"], p["payer"], p["payee"], p["amount"], t["e2e_id"]))
    for item in cats["SEM_REGISTRO_PROVIDER"]:
        stmts.append("UPDATE payments SET status = 'FAILED', updated_at = now() WHERE id = %s "
                     "AND status IN ('PROCESSING', 'UNKNOWN');" % q(item["local"]["id"]))
    for item in cats["LEDGER_FALTANDO"]:
        p = item["local"]
        stmts.append(outbox_sql(p["id"], p["client_key"], p["payer"], p["payee"], p["amount"], p["provider_ref"],
                                republish=True))
    for reason in ("DUPLICADO", "CONFLITO_CHAVE", "DIVERGENCIA_GRAVE"):
        for item in cats[reason]:
            ck = item.get("client_key") or item["local"]["client_key"]
            stmts.append("INSERT INTO quarantine (client_key, reason, details) VALUES (%s, %s, %s::jsonb) "
                         "ON CONFLICT DO NOTHING;" % (q(ck), q(reason), q(json.dumps(item, default=str))))
    return stmts


def describe(item):
    if "transfer" in item and "local" not in item:
        t = item["transfer"]
        return "payment_id=%s client_key=%s provider=%s %s" % (t["payment_id"], t["client_key"], t["status"], t["amount"])
    if "transfers" in item:
        return "client_key=%s liquidacoes=%d (%s)" % (item["client_key"], len(item["transfers"]),
                                                     ", ".join(t["payment_id"][:8] for t in item["transfers"]))
    if "local" in item:
        p = item["local"]
        extra = (" provider=" + item["transfer"]["status"]) if "transfer" in item else ""
        return "payment_id=%s client_key=%s local=%s%s" % (p["id"], p["client_key"], p["status"], extra)
    return json.dumps(item)


def cmd_reconcile(args):
    apply = "--apply" in args
    w = load_world()
    if not w["local_ok"]:
        raise LabError("banco roteado (%s) inacessivel: %s" % (w["host"], w.get("local_error")))
    cats = classify(w)
    print("RECONCILIACAO  banco=%s (%s)  provider=%d transferencias  pagamentos locais=%d  ledger=%d pagamentos" % (
        w["host"], "somente leitura" if w["read_only"] else "escrita", len(w["provider"]), len(w["local"]),
        len(w["ledger"])))
    print()
    total = 0
    for name, meaning, action in CATEGORY_HELP:
        items = cats.get(name, [])
        total += len(items)
        mark = "  " if not items else "->"
        print("%s %-22s %4d   %s" % (mark, name, len(items), meaning))
        if items:
            print("   %-22s        acao: %s" % ("", action))
            for it in items[:5]:
                print("   %-22s        - %s" % ("", describe(it)))
            if len(items) > 5:
                print("   %-22s        ... e mais %d" % ("", len(items) - 5))
    counts = dict((n, len(cats.get(n, []))) for n, _, _ in CATEGORY_HELP)
    print()
    if not apply:
        record("reconciliacao", aplicada=False, contagens=counts)
        print("Nada foi alterado. Revise e rode 'reconcile --apply' para executar as acoes seguras.")
        print("Nenhuma acao reenvia pagamento ao provider.")
        return
    if w["read_only"]:
        raise LabError("o banco roteado esta em modo leitura (replica nao promovida?). Nada foi aplicado.")
    stmts = apply_sql(cats)
    if stmts:
        psql(DB_CONTAINER[w["host"]], "payment", "payment", "BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;", timeout=60)
    record("reconciliacao", aplicada=True, contagens=counts, comandos=len(stmts))
    print("Aplicado: %d comandos em uma transacao. Os eventos novos saem pelo Outbox (precisa do worker e do Kafka)." % len(stmts))
    print("Rode 'check' em seguida.")


def check_results(w, cats):
    res = []  # (id, nome, nivel, detalhe)
    dups = cats.get("DUPLICADO", [])
    res.append(("I1", "Efeito financeiro unico", "FAIL" if dups else "PASS",
                ("%d chave(s) liquidada(s) mais de uma vez: %s" % (len(dups), ", ".join(d["client_key"] for d in dups[:5])))
                if dups else "nenhuma chave do cliente liquidada duas vezes no provider"))

    deb = sum(float(r["debit"]) for r in w["ledger"])
    cred = sum(float(r["credit"]) for r in w["ledger"])
    bad = [r for r in w["ledger"] if int(r["n"]) != 2 or r["debit"] != r["credit"]]
    orphans = cats.get("LEDGER_ORFAO", [])
    ok = abs(deb - cred) < 0.005 and not bad and not orphans
    res.append(("I2", "Ledger consistente", "PASS" if ok else "FAIL",
                "debitos %.2f = creditos %.2f; %d pagamento(s) com lancamento incompleto; %d orfao(s)" % (
                    deb, cred, len(bad), len(orphans))))

    unknown = len(cats.get("AUSENTE_LOCAL", [])) + len(cats.get("CONFLITO_CHAVE", [])) - sum(
        1 for c in cats.get("CONFLITO_CHAVE", []) if c["client_key"] in w["quarantine"])
    res.append(("I3", "Toda operacao conhecida ou em quarentena", "FAIL" if unknown else "PASS",
                "%d transferencia(s) do provider desconhecida(s) pela Tech Pix" % unknown))

    stale = (len(cats.get("ESTADO_DESATUALIZADO", [])) + len(cats.get("SEM_REGISTRO_PROVIDER", [])) +
             sum(1 for c in cats.get("DIVERGENCIA_GRAVE", []) if c["local"]["client_key"] not in w["quarantine"]))
    res.append(("I4", "Estados coerentes com o provider", "FAIL" if stale else "PASS",
                "%d pagamento(s) com estado desatualizado ou divergente" % stale))

    missing = len(cats.get("LEDGER_FALTANDO", []))
    res.append(("I5", "Ledger completo (eventos entregues)", "FAIL" if missing or w["outbox_stuck"] else "PASS",
                "%d COMPLETED sem lancamento; %d evento(s) parado(s) no outbox ha mais de 15s" % (missing, w["outbox_stuck"])))

    promo = last_event("promocao")
    prim = container("lab10-pg-primary")
    split = None
    if not promo:
        res.append(("I6", "Um unico escritor", "PASS", "sem promocao: o unico primary e o pg-primary"))
    else:
        fenced = prim is None or NET not in prim["networks"]
        if fenced:
            res.append(("I6", "Um unico escritor (fencing)", "PASS", "antigo primary isolado da rede"))
        else:
            res.append(("I6", "Um unico escritor (fencing)", "FAIL",
                        ("antigo primary ainda na rede e LIGADO: aceita escrita de quem apontar para ele" if prim["status"] == "running" else "antigo primary ainda na rede (%s): se voltar, aceita escrita" % prim["status"])))
        if prim and prim["status"] == "running":
            try:
                split = int(pay_db("pg-primary", "SELECT count(*) FROM payments WHERE updated_at > to_timestamp(%s)"
                                   % promo["ts"]))
            except LabError:
                split = None
        if split:
            res.append(("I6b", "Escritas no antigo primary apos a promocao", "WARN",
                        "%d escrita(s) de split-brain: explique no relatorio" % split))
        elif prim and prim["status"] != "running":
            res.append(("I6b", "Escritas no antigo primary apos a promocao", "INFO",
                        "nao verificado: antigo primary desligado"))
        else:
            res.append(("I6b", "Escritas no antigo primary apos a promocao", "PASS", "nenhuma"))

    code, ready = http_json(API + "/health/ready")
    open_ = code == 200 and not os.path.exists(os.path.join(RUNTIME, "maintenance")) and not w["read_only"]
    res.append(("I7", "Servico reaberto", "PASS" if open_ else "FAIL",
                "api pronta, banco com escrita e manutencao desligada" if open_ else
                "api: %s | manutencao: %s | banco %s: %s" % (
                    "pronta" if code == 200 else ready.get("reason", "sem resposta"),
                    "ON" if os.path.exists(os.path.join(RUNTIME, "maintenance")) else "off",
                    w["host"], "somente leitura/inacessivel" if w["read_only"] else "escrita")))
    return res, split


def cmd_check(_args):
    w = load_world()
    if not w["local_ok"]:
        print("banco roteado (%s) inacessivel: %s" % (w["host"], w.get("local_error")))
    cats = classify(w)
    res, split = check_results(w, cats)
    ok = all(level != "FAIL" for _, _, level, _ in res)
    recovered = all(level != "FAIL" for rid, _, level, _ in res if rid != "I1")
    print("INVARIANTES  (%s, banco roteado: %s)" % (hms(time.time()), w["host"]))
    for rid, name, level, detail in res:
        print("  [%-4s] %-4s %-42s %s" % (level, rid, name, detail))
    print()
    if ok:
        print("RESULTADO: TODOS OS INVARIANTES OK")
    elif recovered:
        print("RESULTADO: servico recuperado, MAS com efeito financeiro duplicado.")
        print("  Duplicacao nao se desfaz com reconciliacao: exige estorno e contato com o cliente (decisao humana).")
        print("  As chaves estao em quarentena. Registre no relatorio.")
    else:
        print("RESULTADO: AINDA NAO: corrija os itens FAIL")
    record("check", ok=ok, recuperado=recovered, resultados=[{"id": r[0], "nome": r[1], "nivel": r[2], "detalhe": r[3]} for r in res],
           duplicados=len(cats.get("DUPLICADO", [])), split_brain=split)


# ---------------------------------------------------------------------------
# Relatorio
# ---------------------------------------------------------------------------

def cmd_report(args):
    team = "equipe"
    meta = 15.0
    if "--equipe" in args:
        team = args[args.index("--equipe") + 1]
    if "--meta-rto" in args:
        meta = float(args[args.index("--meta-rto") + 1])
    fail = last_event("falha_regiao")
    if not fail:
        raise LabError("nenhuma falha registrada (o instrutor ainda nao rodou 'chaos start')")
    t0 = fail["ts"]
    evs = [e for e in events() if e["ts"] >= t0 - 600]
    detected = next((e for e in evs if e["type"] == "marco" and e.get("evento") == "detectado"), None)
    promo = next((e for e in evs if e["type"] == "promocao" and e["ts"] >= t0), None)
    checks = [e for e in evs if e["type"] == "check" and e["ts"] >= t0]
    first_ok = next((e for e in checks if e.get("recuperado", e["ok"])), None)
    first_rec = next((e for e in evs if e["type"] == "reconciliacao" and e["ts"] >= t0), None)
    last_check = checks[-1] if checks else None
    cut = last_event("replicacao_cortada")

    rto = (first_ok["ts"] - t0) if first_ok else None
    lines = ["# Relatorio do Game Day - %s" % team, "",
             "Gerado em %s pelo `scripts/lab10.sh report`. Valores medidos pelo laboratorio; "
             "as secoes marcadas com [preencher] sao da equipe." % datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "",
             "## Medicoes", "",
             "| Medida | Valor |", "|---|---|",
             "| Falha da regiao | %s |" % hms(t0),
             "| Deteccao (marco `detectado`) | %s |" % (
                 "T+" + mmss(detected["ts"] - t0) if detected else "nao registrada"),
             "| Recuperacao validada (1o `check` com I2-I7 sem FAIL) | %s |" % (
                 "T+" + mmss(rto) if rto is not None else "nao alcancada"),
             "| Meta de recuperacao (RTO) | %s min: %s |" % (
                 ("%g" % meta), ("DENTRO" if rto is not None and rto <= meta * 60 else "FORA")),
             ]
    if promo:
        last_tx = promo.get("ultima_transacao_replica") or 0
        lines.append("| Perda observada (janela RPO) | %s: ultima transacao na replica %s, falha %s |" % (
            mmss(t0 - last_tx) if last_tx else "?", hms(last_tx), hms(t0)))
        lines.append("| Failover autorizado por | %s, as %s |" % (promo.get("autorizado_por"), hms(promo["ts"])))
    else:
        lines.append("| Perda observada (janela RPO) | sem promocao: a equipe nao fez failover |")
    if cut:
        lines.append("| Replicacao cortada (instrutor) | %s |" % hms(cut["ts"]))
    if first_rec:
        c = first_rec["contagens"]
        lines.append("| Transacoes afetadas (1a reconciliacao, T+%s) | %s |" % (
            mmss(first_rec["ts"] - t0), ", ".join("%s=%d" % (k, v) for k, v in c.items() if v) or "nenhuma"))
    else:
        lines.append("| Transacoes afetadas | reconciliacao nao executada |")
    if last_check:
        dups = last_check.get("duplicados", 0)
        lines.append("| Efeitos financeiros duplicados | %s |" % (
            "nenhum" if not dups else "%d chave(s) liquidada(s) em dobro: criterio NAO atendido" % dups))
        if last_check.get("split_brain"):
            lines.append("| Escritas de split-brain no antigo primary | %d |" % last_check["split_brain"])
    lines += ["", "## Invariantes (ultimo `check`%s)" % (" as " + hms(last_check["ts"]) if last_check else ""), ""]
    if last_check:
        lines += ["| | Invariante | Resultado | Detalhe |", "|---|---|---|---|"]
        for r in last_check["resultados"]:
            lines.append("| %s | %s | %s | %s |" % (r["id"], r["nome"], r["nivel"], r["detalhe"]))
    else:
        lines.append("Nenhum `check` executado depois da falha.")
    lines += ["", "## Linha do tempo", "", "| Hora | T+ | Evento |", "|---|---|---|"]
    for e in evs:
        detail = dict((k, v) for k, v in e.items() if k not in ("type", "ts", "resultados", "contagens"))
        if e["type"] == "reconciliacao":
            detail["itens"] = sum(e["contagens"].values())
        if detail.get("ultima_transacao_replica"):
            detail["ultima_transacao_replica"] = hms(detail["ultima_transacao_replica"])
        lines.append("| %s | %s | %s %s |" % (hms(e["ts"]), mmss(e["ts"] - t0), e["type"],
                                            " ".join("%s=%s" % kv for kv in detail.items())))
    lines += ["", "## Decisao e justificativa [preencher]", "",
              "Promovemos (ou nao) a replica porque... Que evidencia usamos? Quem autorizou?", "",
              "## Divergencias e como foram controladas [preencher]", "",
              "Para cada item em quarentena ou WARN: o que aconteceu, impacto no cliente, proxima acao e responsavel.", "",
              "## O que fariamos diferente [preencher]", ""]
    os.makedirs(REPORTS, exist_ok=True)
    path = os.path.join(REPORTS, "game-day-%s-%s.md" % (team, datetime.now().strftime("%Y%m%d-%H%M")))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines[5:16]))
    print()
    print("Relatorio completo: " + os.path.relpath(path, os.getcwd()))


# ---------------------------------------------------------------------------

COMMANDS = {
    "up": cmd_up, "down": cmd_down, "reset": cmd_reset, "status": cmd_status, "logs": cmd_logs,
    "chaos": cmd_chaos, "mark": cmd_mark, "maintenance": cmd_maintenance, "promote": cmd_promote,
    "fence": cmd_fence, "route": cmd_route, "start": lambda a: service_cmd("start", a),
    "restart": lambda a: service_cmd("restart", a), "provider": cmd_provider, "sql": cmd_sql,
    "reconcile": cmd_reconcile, "check": cmd_check, "report": cmd_report,
}


def main(argv):
    if not argv or argv[0] not in COMMANDS:
        print(__doc__)
        return 0 if not argv or argv[0] in ("-h", "--help", "help") else 2
    try:
        COMMANDS[argv[0]](argv[1:])
    except LabError as e:
        print("erro: %s" % e)
        return 1
    except KeyboardInterrupt:
        print("\ninterrompido.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
