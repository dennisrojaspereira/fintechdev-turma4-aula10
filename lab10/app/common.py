"""Codigo compartilhado pelos servicos do lab 10: log estruturado, roteamento, bancos e Kafka."""

import json
import os
import time
from datetime import datetime, timezone

import psycopg

RUNTIME = "/runtime"
TOPIC = "payment-events"


def log(service, level, message, **fields):
    rec = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
           "service": service, "level": level, "message": message}
    rec.update(fields)
    print(json.dumps(rec, default=str), flush=True)


class RateLimitedLog:
    """Evita uma linha de erro por segundo quando uma dependencia esta fora."""

    def __init__(self, service, every=10):
        self.service, self.every = service, every
        self.last_msg, self.last_ts = None, 0.0

    def error(self, message, **fields):
        now = time.time()
        key = message + str(fields.get("error", ""))
        if key != self.last_msg or now - self.last_ts >= self.every:
            log(self.service, "ERROR", message, **fields)
            self.last_msg, self.last_ts = key, now


def runtime_file(name):
    return os.path.join(RUNTIME, name)


def read_runtime(name, default=None):
    try:
        with open(runtime_file(name), encoding="utf-8") as f:
            value = f.read().strip()
            return value if value else default
    except OSError:
        return default


def write_heartbeat(name, data):
    data = dict(data, ts=time.time())
    tmp = runtime_file(name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str)
        os.chmod(tmp, 0o666)
        os.replace(tmp, runtime_file(name))
    except OSError:
        pass


def payment_db_host():
    """Roteamento: qual banco e o "payment-db" agora. O controlador escreve este arquivo."""
    return read_runtime("payment-db", "pg-primary")


def connect_payment(host):
    return psycopg.connect(host=host, dbname="payment", user="payment", password="payment",
                           connect_timeout=2)


def connect_ledger():
    return psycopg.connect(host="pg-ledger", dbname="ledger", user="ledger", password="ledger",
                           connect_timeout=2)


def ensure_topic(bootstrap, on_error=None):
    """Cria o topico com 3 particoes (auto-create esta desligado). Bloqueia ate o Kafka responder."""
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})
    while True:
        try:
            if TOPIC in admin.list_topics(timeout=5).topics:
                return
            admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result(10)
            return
        except Exception as e:  # noqa: BLE001 - qualquer falha: Kafka ainda nao esta pronto
            if "TOPIC_ALREADY_EXISTS" in str(e):
                return
            if on_error:
                on_error(e)
            time.sleep(2)
