"""Payment API da Tech Pix (lab 10).

  POST /pix              header Idempotency-Key + {payer, payee, amount} -> 202 PROCESSING | UNKNOWN
  GET  /pix/<chave>      estado do pagamento pela chave do cliente
  GET  /health           liveness: o PROCESSO esta vivo (sempre UP; nao olha o banco!)
  GET  /health/ready     readiness: banco roteado acessivel e aceitando escrita
  GET  /internal/info    banco em uso e modo manutencao

O banco e escolhido a cada requisicao pelo roteamento (/runtime/payment-db).
Defeito de projeto (de proposito): o provider recebe o id INTERNO do pagamento como chave de
idempotencia, nao a chave do cliente. Se o registro do pagamento se perder, um reenvio do
cliente vira um pagamento novo para o provider.
"""

import json
import os
import urllib.error
import urllib.request
import uuid
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import psycopg

from common import connect_payment, log, payment_db_host, runtime_file

PROVIDER_URL = os.environ.get("PROVIDER_URL", "http://provider:8090")
PROVIDER_TIMEOUT = float(os.environ.get("PROVIDER_TIMEOUT", "1.5"))
COLUMNS = "id, client_key, payer, payee, amount, status, provider_ref, origin, created_at, updated_at"


def maintenance():
    return os.path.exists(runtime_file("maintenance"))


def row_to_dict(row):
    keys = [c.strip() for c in COLUMNS.split(",")]
    d = dict(zip(keys, row))
    d["id"], d["amount"] = str(d["id"]), str(d["amount"])
    d["created_at"], d["updated_at"] = d["created_at"].isoformat(), d["updated_at"].isoformat()
    return d


def find(conn, client_key):
    row = conn.execute("SELECT " + COLUMNS + " FROM payments WHERE client_key = %s", (client_key,)).fetchone()
    return row_to_dict(row) if row else None


def call_provider(payment):
    body = json.dumps({
        "idempotency_key": payment["id"],
        "payer": payment["payer"], "payee": payment["payee"], "amount": payment["amount"],
        "metadata": {"payment_id": payment["id"], "client_key": payment["client_key"]},
    }).encode()
    req = urllib.request.Request(PROVIDER_URL + "/transfers", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=PROVIDER_TIMEOUT) as resp:
        return json.loads(resp.read())


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"status": "UP"})
        host = payment_db_host()
        if self.path == "/internal/info":
            return self._send(200, {"db_target": host, "maintenance": maintenance()})
        if self.path == "/health/ready":
            try:
                with connect_payment(host) as conn:
                    read_only = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
                if read_only:
                    return self._send(503, {"status": "DOWN", "db": host, "reason": "banco em modo leitura (replica)"})
                return self._send(200, {"status": "UP", "db": host, "maintenance": maintenance()})
            except psycopg.Error as e:
                return self._send(503, {"status": "DOWN", "db": host, "reason": str(e).splitlines()[0]})
        if self.path.startswith("/pix/"):
            key = unquote(self.path[len("/pix/"):])
            try:
                with connect_payment(host) as conn:
                    p = find(conn, key)
            except psycopg.Error as e:
                return self._send(503, {"error": "DB_UNAVAILABLE", "db": host, "detail": str(e).splitlines()[0]})
            return self._send(200, p) if p else self._send(404, {"error": "NOT_FOUND", "client_key": key})
        self._send(404, {"error": "NOT_FOUND"})

    def do_POST(self):
        if self.path != "/pix":
            return self._send(404, {"error": "NOT_FOUND"})
        if maintenance():
            return self._send(503, {"error": "MAINTENANCE", "message": "pagamentos temporariamente suspensos"})
        key = self.headers.get("Idempotency-Key")
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            amount = Decimal(str(req["amount"])).quantize(Decimal("0.01"))
            payer, payee = req["payer"], req["payee"]
        except (ValueError, KeyError, InvalidOperation):
            return self._send(400, {"error": "BAD_REQUEST"})
        if not key:
            return self._send(400, {"error": "IDEMPOTENCY_KEY_REQUIRED"})

        host = payment_db_host()
        try:
            with connect_payment(host) as conn:
                existing = find(conn, key)
                if existing:
                    return self._send(200, existing)
                pid = str(uuid.uuid4())
                conn.execute("INSERT INTO payments (id, client_key, payer, payee, amount, status) "
                             "VALUES (%s, %s, %s, %s, %s, 'PROCESSING') ON CONFLICT (client_key) DO NOTHING",
                             (pid, key, payer, payee, amount))
                conn.commit()
                payment = find(conn, key)
        except psycopg.Error as e:
            log("payment-api", "ERROR", "banco indisponivel", client_key=key, db=host,
                error=str(e).splitlines()[0])
            return self._send(503, {"error": "DB_UNAVAILABLE", "db": host})
        if payment["id"] != pid:  # corrida com outro reenvio da mesma chave
            return self._send(200, payment)

        log("payment-api", "INFO", "pagamento recebido", payment_id=pid, client_key=key,
            amount=str(amount), db=host)
        try:
            transfer = call_provider(payment)
            log("payment-api", "INFO", "provider aceitou", payment_id=pid, e2e_id=transfer.get("e2e_id"))
            return self._send(202, payment)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # Sem resposta nao significa que o provider nao processou.
            log("payment-api", "WARN", "sem resposta do provider: estado indeterminado",
                payment_id=pid, client_key=key, status="UNKNOWN", error=str(e))
            try:
                with connect_payment(host) as conn:
                    conn.execute("UPDATE payments SET status = 'UNKNOWN', updated_at = now() "
                                 "WHERE id = %s AND status = 'PROCESSING'", (pid,))
            except psycopg.Error:
                pass
            payment["status"] = "UNKNOWN"
            return self._send(202, payment)


if __name__ == "__main__":
    log("payment-api", "INFO", "payment-api no ar", port=8088, db=payment_db_host())
    ThreadingHTTPServer(("0.0.0.0", 8088), Handler).serve_forever()
