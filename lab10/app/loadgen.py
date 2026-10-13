"""Clientes da Tech Pix (lab 10).

Cada cliente: envia um Pix com uma Idempotency-Key, acompanha ate COMPLETED/FAILED e paga de novo.
  - 503 ou erro de conexao: espera e tenta de novo (o cliente nao desiste facil)
  - 404 ao consultar um pagamento que ele ENVIOU: "sumiu" -> reenvia com a MESMA chave

Quantidade de clientes ativos: /runtime/loadgen-clients (padrao 6; o desastre dobra para 12).
"""

import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid

from common import log, read_runtime

API = os.environ.get("API_URL", "http://payment-api:8088")
MAX_CLIENTS = 12
GIVE_UP_S = 900


def active_clients():
    try:
        return int(read_runtime("loadgen-clients", "6"))
    except ValueError:
        return 6


def http(method, path, body=None, key=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except ValueError:
            return e.code, {}
    except (urllib.error.URLError, OSError):
        return 0, {}


def submit(key, body, deadline):
    while time.time() < deadline:
        code, _ = http("POST", "/pix", body, key)
        if code in (200, 202):
            return True
        if 400 <= code < 500:
            return False
        time.sleep(2)
    return False


def client(i):
    time.sleep(i * 0.3)
    while True:
        if i >= active_clients():
            time.sleep(1)
            continue
        key = "cli-%02d-%s" % (i, uuid.uuid4().hex[:10])
        body = {"payer": "ACC-%04d" % random.randint(1000, 1099),
                "payee": "ACC-%04d" % random.randint(2000, 2099),
                "amount": random.choice([25, 40, 75, 99.9, 120, 250, 310.5, 480, 1200])}
        deadline = time.time() + GIVE_UP_S
        if not submit(key, body, deadline):
            continue
        while time.time() < deadline:
            time.sleep(1)
            code, data = http("GET", "/pix/" + key)
            if code == 200 and data.get("status") in ("COMPLETED", "FAILED"):
                break
            if code == 404:
                log("loadgen", "WARN", "cliente nao encontrou o pagamento que enviou; reenviando com a mesma chave",
                    client_key=key)
                submit(key, body, deadline)
        time.sleep(0.5)


if __name__ == "__main__":
    log("loadgen", "INFO", "clientes no ar", api=API, max_clients=MAX_CLIENTS)
    for n in range(MAX_CLIENTS):
        threading.Thread(target=client, args=(n,), daemon=True).start()
    while True:
        time.sleep(60)
