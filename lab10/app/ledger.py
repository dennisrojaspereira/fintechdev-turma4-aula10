"""Ledger (lab 10): consome PaymentCompleted e registra debito + credito.

Idempotente: o event_id entra em processed_events na MESMA transacao dos lancamentos.
O offset so e confirmado depois do commit no banco. Replay e seguro; evento repetido e ignorado.
"""

import json
import os
import time
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer

from common import TOPIC, RateLimitedLog, connect_ledger, ensure_topic, log, write_heartbeat

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
STATE = {"processed": 0, "duplicates_ignored": 0, "last_event_ts": None, "kafka_ok": False,
         "db_ok": True, "last_error": None}
LAST_KAFKA_ERROR = [0.0]
ERR = RateLimitedLog("ledger")


def kafka_error(err):
    STATE["kafka_ok"] = False
    LAST_KAFKA_ERROR[0] = time.time()
    ERR.error("kafka indisponivel", error=str(err))


def apply_event(conn, event_id, event):
    with conn.transaction():
        cur = conn.execute("INSERT INTO processed_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
                           (event_id,))
        if cur.rowcount == 0:
            return False
        amount = Decimal(event["amount"])
        conn.execute("INSERT INTO ledger_entries (payment_id, account, direction, amount) VALUES "
                     "(%s, %s, 'DEBIT', %s), (%s, %s, 'CREDIT', %s)",
                     (event["payment_id"], event["payer"], amount, event["payment_id"], event["payee"], amount))
        return True


def main():
    ensure_topic(BOOTSTRAP, on_error=kafka_error)
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ledger", "log_level": 0,
                         "error_cb": kafka_error, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    conn = None
    last_hb = 0.0
    log("ledger", "INFO", "ledger no ar", topic=TOPIC)
    while True:
        if time.time() - last_hb > 2:
            STATE["kafka_ok"] = time.time() - LAST_KAFKA_ERROR[0] > 10  # 10s sem erro do cliente Kafka
            write_heartbeat("ledger.json", STATE)
            last_hb = time.time()
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            kafka_error(msg.error())
            continue
        STATE["kafka_ok"] = True
        headers = dict((k, v.decode()) for k, v in (msg.headers() or []))
        event_id = headers.get("event_id")
        event = json.loads(msg.value())
        while True:  # sem banco, nao avanca o offset: tenta de novo
            try:
                if conn is None or conn.closed:
                    conn = connect_ledger()
                    conn.autocommit = True
                if apply_event(conn, event_id, event):
                    STATE["processed"] += 1
                    log("ledger", "INFO", "lancamentos registrados", event_id=event_id,
                        payment_id=event["payment_id"], amount=event["amount"])
                else:
                    STATE["duplicates_ignored"] += 1
                    log("ledger", "INFO", "evento repetido ignorado (idempotencia)", event_id=event_id)
                STATE["db_ok"], STATE["last_event_ts"] = True, time.time()
                break
            except psycopg.Error as e:
                STATE["db_ok"], STATE["last_error"] = False, str(e).splitlines()[0]
                ERR.error("ledger sem banco", error=STATE["last_error"])
                conn = None
                time.sleep(1)
        try:
            consumer.commit(message=msg, asynchronous=False)
        except Exception as e:  # noqa: BLE001 - KafkaException: o evento volta no replay, e tudo bem
            kafka_error(e)


if __name__ == "__main__":
    main()
