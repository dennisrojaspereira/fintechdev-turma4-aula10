"""Payment worker (lab 10): duas tarefas em threads separadas.

  liquidacao  pagamentos PROCESSING/UNKNOWN -> consulta o provider -> COMPLETED/FAILED (+ evento no Outbox)
  relay       Outbox -> Kafka (topico payment-events); marca published_at so depois da confirmacao

ATENCAO (e parte do exercicio): o banco e lido do roteamento UMA VEZ, na inicializacao.
Trocar o roteamento nao muda este processo. Ele continua escrevendo onde estava.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request

import psycopg
from confluent_kafka import Producer

from common import (TOPIC, RateLimitedLog, connect_payment, ensure_topic, log, payment_db_host,
                    write_heartbeat)

PROVIDER_URL = os.environ.get("PROVIDER_URL", "http://provider:8090")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DB_HOST = payment_db_host()          # lido no boot. De proposito.
UNKNOWN_GIVE_UP_S = 20               # sem registro no provider depois disso: nunca foi enviado

STATE = {"db_target": DB_HOST, "db_ok": False, "kafka_ok": False, "settled": 0, "published": 0,
         "last_error": None}
ERR = RateLimitedLog("payment-worker")


def provider_get(payment_id):
    try:
        with urllib.request.urlopen(PROVIDER_URL + "/transfers/" + payment_id, timeout=2) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def completed_event(row, e2e_id):
    pid, client_key, payer, payee, amount = row[0], row[1], row[2], row[3], row[4]
    return (pid + ":PaymentCompleted", "PaymentCompleted",
            json.dumps({"payment_id": pid, "client_key": client_key, "payer": payer, "payee": payee,
                        "amount": str(amount), "e2e_id": e2e_id}))


def settle_once(conn):
    rows = conn.execute(
        "SELECT id::text, client_key, payer, payee, amount, status, "
        "       extract(epoch FROM now() - created_at) "
        "FROM payments WHERE status IN ('PROCESSING', 'UNKNOWN') "
        "AND created_at < now() - interval '1 second' ORDER BY created_at LIMIT 200").fetchall()
    conn.commit()
    for row in rows:
        pid, status, age = row[0], row[5], row[6]
        transfer = provider_get(pid)
        if transfer is None:
            if age > UNKNOWN_GIVE_UP_S:
                conn.execute("UPDATE payments SET status = 'FAILED', updated_at = now() "
                             "WHERE id = %s AND status = %s", (pid, status))
                conn.commit()
                log("payment-worker", "INFO", "provider nunca recebeu: FAILED", payment_id=pid, db=DB_HOST)
            continue
        if transfer["status"] == "PENDING":
            continue
        cur = conn.execute("UPDATE payments SET status = %s, provider_ref = %s, updated_at = now() "
                           "WHERE id = %s AND status IN ('PROCESSING', 'UNKNOWN')",
                           (transfer["status"], transfer["e2e_id"], pid))
        if cur.rowcount == 1 and transfer["status"] == "COMPLETED":
            conn.execute("INSERT INTO outbox (event_id, event_type, payload) VALUES (%s, %s, %s) "
                         "ON CONFLICT (event_id) DO NOTHING", completed_event(row, transfer["e2e_id"]))
        conn.commit()
        if cur.rowcount == 1:
            STATE["settled"] += 1
            log("payment-worker", "INFO", "pagamento liquidado", payment_id=pid,
                status=transfer["status"], e2e_id=transfer["e2e_id"], db=DB_HOST)


def settle_loop():
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = connect_payment(DB_HOST)
            settle_once(conn)
            STATE["db_ok"] = True
        except psycopg.Error as e:
            STATE["db_ok"] = False
            STATE["last_error"] = str(e).splitlines()[0] if str(e) else type(e).__name__
            ERR.error("liquidacao sem banco", db=DB_HOST, error=STATE["last_error"])
            if conn is not None:
                try:
                    conn.close()
                except psycopg.Error:
                    pass
            conn = None
        except (urllib.error.URLError, OSError) as e:
            STATE["last_error"] = "provider: " + str(e)
            ERR.error("liquidacao sem provider", error=str(e))
        time.sleep(1)


def relay_loop():
    def kafka_error(err):
        STATE["kafka_ok"] = False
        ERR.error("kafka indisponivel", error=str(err))

    ensure_topic(BOOTSTRAP, on_error=kafka_error)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "log_level": 0, "error_cb": kafka_error,
                         "message.timeout.ms": 8000, "enable.idempotence": True})
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = connect_payment(DB_HOST)
            rows = conn.execute("SELECT id, event_id, payload::text FROM outbox WHERE published_at IS NULL "
                                "ORDER BY id LIMIT 200").fetchall()
            conn.commit()
            delivered = []
            for oid, event_id, payload in rows:
                pid = event_id.split(":")[0]

                def ack(err, _msg, oid=oid):
                    if err is None:
                        delivered.append(oid)

                producer.produce(TOPIC, key=pid, value=payload,
                                 headers={"event_id": event_id, "event_type": "PaymentCompleted"},
                                 on_delivery=ack)
            if rows:
                producer.flush(10)
            if delivered:
                conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)", (delivered,))
                conn.commit()
                STATE["published"] += len(delivered)
            if rows and len(delivered) == len(rows):
                STATE["kafka_ok"] = True
            elif not rows:
                STATE["kafka_ok"] = len(producer.list_topics(timeout=2).brokers) > 0
        except psycopg.Error as e:
            STATE["last_error"] = str(e).splitlines()[0]
            ERR.error("relay sem banco", db=DB_HOST, error=STATE["last_error"])
            conn = None
        except Exception as e:  # noqa: BLE001 - KafkaException e afins
            STATE["kafka_ok"] = False
            ERR.error("relay sem kafka", error=str(e))
        time.sleep(0.5)


if __name__ == "__main__":
    log("payment-worker", "INFO", "worker no ar (banco lido na inicializacao)", db=DB_HOST)
    threading.Thread(target=settle_loop, daemon=True).start()
    threading.Thread(target=relay_loop, daemon=True).start()
    while True:
        write_heartbeat("worker.json", STATE)
        time.sleep(2)
