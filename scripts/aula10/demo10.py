#!/usr/bin/env python3
"""Aula 10 - demo guiada do professor: perda de dados no failover (RPO).

Uma historia em 5 atos, projetada na tela. O professor aperta Enter entre os atos.
  1. Tudo normal: a replica copia o banco principal quase em tempo real
  2. A replica fica para tras (a replicacao e cortada)
  3. A regiao cai: o banco principal some
  4. Decisao: promover a replica. Quantos pagamentos ficaram para tras?
  5. Recuperar pelo provider (a fonte da verdade) e reabrir

Uso:  scripts/lab10.sh demo [--atraso 30]
      ou no navegador: python scripts/aula10/painel10.py -> http://localhost:8098/demo
Antes de repetir a demo: scripts/lab10.sh reset
"""

import json
import os
import sys
import time

import lab10 as L

WIDTH = 40


def clear():
    if os.name == "nt":
        os.system("cls")
    else:
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()


def title(n, text):
    clear()
    print("=" * 70)
    print("  ATO %d de 5 - %s" % (n, text))
    print("=" * 70)
    print()


def say(*lines):
    for line in lines:
        print("  " + line)
    print()


def pause(msg="Enter para continuar"):
    try:
        input("  [%s] " % msg)
    except EOFError:
        print()


# ---------------------------------------------------------------------------
# Numeros que a turma ve
# ---------------------------------------------------------------------------

def count_db(host):
    try:
        return int(L.pay_db(host, "SELECT count(*) FROM payments"))
    except L.LabError:
        return None


def provider_transfers():
    code, transfers = L.http_json(L.PROVIDER + "/transfers")
    if code != 200:
        raise L.LabError("provider sem resposta")
    return transfers


def replica_lag():
    try:
        v = L.pay_db("pg-replica", "SELECT coalesce(extract(epoch FROM now() - pg_last_xact_replay_timestamp()), 0)")
        return float(v)
    except (L.LabError, ValueError):
        return None


def bar(value, top):
    if value is None:
        return "[ FORA DO AR ]".ljust(WIDTH)
    n = int(round(WIDTH * value / top)) if top else 0
    return "#" * n + "." * (WIDTH - n)


def board(base, primary_up=True):
    """Tres linhas: provider, principal, replica. Mostra o que cada um recebeu desde o inicio da demo."""
    prov = len(provider_transfers()) - base["provider"]
    prim = (count_db("pg-primary") - base["primary"]) if primary_up else None
    repl = count_db("pg-replica")
    repl = None if repl is None else repl - base["replica"]
    top = max(v for v in (prov, prim, repl, 1) if v is not None)
    print("  Pix recebidos desde o inicio da demo:")
    print()
    print("  Provider (fora da TechPix)%s %5d" % (bar(prov, top), prov))
    print("  Banco principal           %s %5s" % (bar(prim, top), "-" if prim is None else prim))
    print("  Replica                   %s %5d" % (bar(repl, top), repl))
    print()
    if prim is not None:
        lag = replica_lag()
        if lag is not None:
            print("  Atraso da replica: %.0f s" % lag)
        print("  So no principal (ainda nao copiados para a replica): %d" % max(prim - repl, 0))
    else:
        print("  O provider tem %d Pix que a replica nao tem." % max(prov - repl, 0))
    print()
    return prov, prim, repl


def live(base, seconds, header, primary_up=True):
    end = time.time() + seconds
    while True:
        clear()
        print(header)
        board(base, primary_up)
        left = int(end - time.time())
        if left <= 0:
            return
        print("  (atualizando... %d s)" % left)
        time.sleep(2)


def lost_payments():
    """Transferencias que o provider tem e o banco (agora a replica) nao conhece."""
    known = set(r["id"] for r in L.query_json("lab10-pg-replica", "payment", "payment", "SELECT id::text AS id FROM payments"))
    return [t for t in provider_transfers() if t.get("payment_id") not in known]


# ---------------------------------------------------------------------------
# A historia
# ---------------------------------------------------------------------------

def preflight():
    if not L.running("lab10-pg-primary") or not L.running("lab10-pg-replica"):
        raise L.LabError("o ambiente nao esta no estado inicial. Rode 'scripts/lab10.sh reset' antes da demo.")
    if L.pay_db("pg-replica", "SELECT pg_is_in_recovery()") != "t":
        raise L.LabError("a replica ja foi promovida numa demo anterior. Rode 'scripts/lab10.sh reset'.")
    if L.pay_db("pg-primary", "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'") == "0":
        raise L.LabError("a replica nao esta copiando o principal. Rode 'scripts/lab10.sh reset'.")
    if not L.running("lab10-loadgen"):
        L.sh(["docker", "start", "lab10-loadgen"])


# ---------------------------------------------------------------------------
# Passos (usados pelo terminal e pela pagina /demo do painel10)
# ---------------------------------------------------------------------------

DEMO_FILE = "demo.json"


def load_demo():
    try:
        return json.loads(L.read_runtime(DEMO_FILE, "{}"))
    except ValueError:
        return {}


def save_demo(**changes):
    d = load_demo()
    d.update(changes)
    L.write_runtime(DEMO_FILE, json.dumps(d))
    return d


def begin():
    preflight()
    L.remove_runtime("maintenance")
    base = {"provider": len(provider_transfers()), "primary": count_db("pg-primary"), "replica": count_db("pg-replica")}
    L.write_runtime(DEMO_FILE, json.dumps({"base": base, "stage": "normal"}))
    return base


def snapshot():
    """Os numeros da tela: o que cada lugar recebeu desde o inicio da demo."""
    d = load_demo()
    base = d.get("base")
    if not base:
        return {"stage": None}
    prim = count_db("pg-primary") if d.get("stage") in ("normal", "cortada") else None
    repl = count_db("pg-replica")
    return {"stage": d.get("stage"),
            "provider": len(provider_transfers()) - base["provider"],
            "primary": None if prim is None else prim - base["primary"],
            "replica": None if repl is None else repl - base["replica"],
            "lag": replica_lag() if prim is not None else None,
            "promocao": d.get("promocao"), "reconciliacao": d.get("reconciliacao")}


def cut():
    L.pay_db("pg-primary", "ALTER ROLE replicator NOLOGIN; "
                           "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_replication")
    L.record("replicacao_cortada")
    save_demo(stage="cortada")


def crash():
    L.sh(["docker", "kill", "lab10-pg-primary"])
    L.sh(["docker", "kill", "lab10-loadgen"])
    L.record("falha_regiao")
    save_demo(stage="queda")


def promote():
    last = float(L.pay_db("pg-replica", "SELECT coalesce(extract(epoch FROM pg_last_xact_replay_timestamp()), 0)") or 0)
    fail = L.last_event("falha_regiao")
    L.sh(["docker", "network", "disconnect", "-f", L.NET, "lab10-pg-primary"])  # fencing: fora do roteiro da demo
    L.record("fencing", alvo="primary", estado_container="exited")
    L.pay_db("pg-replica", "SELECT pg_promote(true, 60)")
    L.write_runtime("payment-db", "pg-replica")
    L.sh(["docker", "restart", "lab10-payment-worker"])
    L.record("promocao", autorizado_por="professor (demo)", ultima_transacao_replica=last, primary_isolado=True)
    L.record("roteamento", destino="pg-replica")
    lost = lost_payments()
    paid = [t for t in lost if t["status"] == "COMPLETED"]
    result = {"ultimo_dado": L.hms(last), "queda": L.hms(fail["ts"]), "janela": L.mmss(fail["ts"] - last),
              "janela_s": round(fail["ts"] - last), "perdidos": len(lost), "pagos": len(paid),
              "valor": round(sum(float(t["amount"]) for t in paid), 2),
              "exemplos": [{"hora": L.hms(t["created_at"]), "valor": t["amount"], "de": t["payer"], "para": t["payee"]}
                           for t in paid[:3]]}
    save_demo(stage="promovida", promocao=result)
    return result


def reconcile():
    before = len(lost_payments())
    stmts = L.apply_sql(L.classify(L.load_world()))
    if stmts:
        L.psql("lab10-pg-replica", "payment", "payment", "BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;", timeout=60)
    L.record("reconciliacao", aplicada=True, comandos=len(stmts))
    time.sleep(2)
    after = len(lost_payments())
    L.sh(["docker", "start", "lab10-loadgen"])
    result = {"antes": before, "depois": after}
    save_demo(stage="reconciliada", reconciliacao=result)
    return result


def run(args):
    atraso = int(args[args.index("--atraso") + 1]) if "--atraso" in args else 30
    base = begin()

    # ATO 1 ----------------------------------------------------------------
    title(1, "Tudo normal")
    say("A Tech Pix recebe Pix o tempo todo. Cada Pix vai para dois lugares:",
        "  - o PROVIDER, que faz o dinheiro sair de verdade (fica fora da Tech Pix);",
        "  - o BANCO PRINCIPAL da Tech Pix, que guarda o registro do pagamento.",
        "",
        "O banco principal tem uma REPLICA: uma copia em outro lugar, atualizada",
        "continuamente. Ela existe para o dia em que o principal morrer.")
    pause("Enter para ver os numeros ao vivo")
    live(base, 12, "\n  ATO 1 - Tudo normal: as tres barras andam juntas\n")
    say("As barras andam juntas. A replica esta praticamente em dia (atraso ~0 s).")
    pause()

    # ATO 2 ----------------------------------------------------------------
    title(2, "A replica fica para tras")
    say("Problema silencioso: a copia para a replica para de chegar",
        "(rede ruim, disco cheio, replica sobrecarregada...).",
        "",
        "Ninguem percebe: os clientes continuam pagando, o principal continua",
        "gravando. So a replica fica parada no tempo.",
        "",
        "Observem a barra da replica.")
    pause("Enter para cortar a replicacao")
    cut()
    live(base, atraso, "\n  ATO 2 - A replica parou de receber dados\n")
    say("O principal continua crescendo; a replica congelou.",
        "A diferenca entre as duas barras e o que AINDA NAO TEM COPIA.")
    pause()

    # ATO 3 ----------------------------------------------------------------
    title(3, "A regiao principal cai")
    say("Sexta-feira, 18h. O datacenter do banco principal cai.",
        "Os clientes ficam aguardando (pausamos os clientes para focar no banco).")
    pause("Enter para derrubar o banco principal")
    crash()
    clear()
    print("\n  ATO 3 - O banco principal esta FORA DO AR\n")
    board(base, primary_up=False)
    say("O principal sumiu, e com ele os Pix que so existiam la.",
        "O provider NAO caiu: ele e de outra empresa. O dinheiro desses Pix ja saiu.",
        "",
        "Pergunta para a turma: e agora? Esperamos o principal voltar",
        "(sem saber quando) ou promovemos a replica (sabendo que ela esta atrasada)?")
    pause("Enter para promover a replica")

    # ATO 4 ----------------------------------------------------------------
    title(4, "Promover a replica: o preco do atraso")
    r = promote()
    say("A replica agora e o banco da Tech Pix. O sistema voltou. Mas:")
    print("  +------------------------------------------------------------+")
    print("  |  Ultimo dado copiado para a replica: %-22s|" % r["ultimo_dado"])
    print("  |  Queda do principal:                 %-22s|" % r["queda"])
    print("  |  Janela perdida (RPO observado):     %-22s|" % r["janela"])
    print("  |                                                            |")
    print("  |  Pix que o provider tem e a Tech Pix NAO conhece: %-9d|" % r["perdidos"])
    print("  |  ... ja pagos (dinheiro saiu):  %4d   R$ %-17s|" % (r["pagos"], "%.2f" % r["valor"]))
    print("  +------------------------------------------------------------+")
    print()
    if r["exemplos"]:
        print("  Exemplos:")
        for t in r["exemplos"]:
            print("    %s  R$ %8s  %s -> %s   (provider: PAGO | Tech Pix: nao existe)" % (
                t["hora"], t["valor"], t["de"], t["para"]))
        print()
    say("Isso e o RPO: quanto dado aceitamos perder no failover.",
        "Replica assincrona = RPO maior que zero. A janela perdida e exatamente",
        "o atraso que a replica tinha quando o principal caiu.")
    pause()

    # ATO 5 ----------------------------------------------------------------
    title(5, "Recuperar o que se perdeu")
    say("Esses pagamentos sumiram do NOSSO banco, mas nao do mundo:",
        "o provider sabe de todos. Ele e a fonte da verdade do dinheiro.",
        "",
        "Reconciliar = comparar provider x banco e reconstruir o que falta,",
        "com o MESMO id. Nada e reenviado ao provider (senao o dinheiro sairia 2x).")
    pause("Enter para reconciliar")
    rc = reconcile()
    print("  Pix que o provider tem e a Tech Pix nao conhece:  antes %d  ->  agora %d" % (rc["antes"], rc["depois"]))
    print()
    say("Clientes de volta. A Tech Pix reabriu sem perder nenhum pagamento.")
    pause("Enter para o resumo")

    clear()
    print("=" * 70)
    print("  RESUMO")
    print("=" * 70)
    print()
    say("1. Replica assincrona fica atrasada. Quando o principal cai, o atraso vira perda.",
        "2. RPO = a perda que aceitamos. Aqui: %s de dados, %d Pix." % (r["janela"], r["perdidos"]),
        "3. Promover e uma troca: volta rapido (RTO baixo) aceitando perder dados (RPO > 0).",
        "4. Perder no banco nao e perder no mundo: com uma fonte externa (o provider)",
        "   da para reconstruir. Sem ela, a perda seria definitiva.",
        "",
        "Para discutir:",
        "  - Qual RPO a Tech Pix aceita? Quanto custa chegar a zero (replica sincrona)?",
        "  - Como descobrir ANTES da queda que a replica estava atrasada? (alerta de lag)",
        "  - E se o principal voltar depois da promocao? (dois bancos aceitando escrita)")
    say("Para repetir a demo: scripts/lab10.sh reset")


if __name__ == "__main__":
    try:
        run(sys.argv[1:])
    except L.LabError as e:
        print("erro: %s" % e)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\ninterrompido.")
        sys.exit(130)
