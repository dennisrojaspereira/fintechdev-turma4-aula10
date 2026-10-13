"""Provider externo simulado (o PSP que liquida o Pix).

Fica FORA da regiao da Tech Pix: continua aceitando e liquidando transferencias durante o desastre.

  POST /transfers            {idempotency_key, payer, payee, amount, metadata} -> 202 (ou 200 se a chave ja existe)
  GET  /transfers/<chave>    estado de uma transferencia (fonte de verdade do efeito financeiro)
  GET  /transfers            todas as transferencias (usado na reconciliacao)

Comportamento: aceita, responde em 50-300 ms (3% das vezes demora 2,5 s DEPOIS de registrar:
a Tech Pix sofre timeout, mas a transferencia existe) e liquida 1-4 s depois (95% COMPLETED).
"""

import json
import random
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

from common import log

DB_PATH = "/data/provider.db"
LOCK = threading.Lock()
DB = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("""CREATE TABLE IF NOT EXISTS transfers (
    idempotency_key TEXT PRIMARY KEY,
    e2e_id TEXT NOT NULL,
    payment_id TEXT, client_key TEXT,
    payer TEXT, payee TEXT, amount TEXT,
    status TEXT NOT NULL,           -- PENDING | COMPLETED | FAILED
    outcome TEXT NOT NULL,
    created_at REAL NOT NULL, settle_at REAL NOT NULL, settled_at REAL)""")
SEQ = [DB.execute("SELECT count(*) FROM transfers").fetchone()[0]]


def settle_due():
    now = time.time()
    with LOCK:
        DB.execute("UPDATE transfers SET status = outcome, settled_at = ? "
                   "WHERE status = 'PENDING' AND settle_at <= ?", (now, now))


def settler():
    while True:
        try:
            settle_due()
        except sqlite3.Error as e:
            log("provider", "ERROR", "falha ao liquidar", error=str(e))
        time.sleep(0.3)


def as_dict(row):
    d = dict(row)
    d.pop("outcome", None)
    d.pop("settle_at", None)
    return d


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
        settle_due()
        if self.path == "/transfers":
            with LOCK:
                rows = DB.execute("SELECT * FROM transfers ORDER BY created_at").fetchall()
            return self._send(200, [as_dict(r) for r in rows])
        if self.path.startswith("/transfers/"):
            key = unquote(self.path[len("/transfers/"):])
            with LOCK:
                row = DB.execute("SELECT * FROM transfers WHERE idempotency_key = ?", (key,)).fetchone()
            return self._send(200, as_dict(row)) if row else self._send(404, {"error": "NOT_FOUND"})
        self._send(404, {"error": "NOT_FOUND"})

    def do_POST(self):
        if self.path != "/transfers":
            return self._send(404, {"error": "NOT_FOUND"})
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            key = req["idempotency_key"]
        except (ValueError, KeyError):
            return self._send(400, {"error": "BAD_REQUEST"})
        meta = req.get("metadata") or {}
        now = time.time()
        with LOCK:
            row = DB.execute("SELECT * FROM transfers WHERE idempotency_key = ?", (key,)).fetchone()
            if row:
                created = False
            else:
                SEQ[0] += 1
                e2e = "E%s%06d" % (time.strftime("%Y%m%d%H%M%S", time.gmtime(now)), SEQ[0])
                outcome = "COMPLETED" if random.random() < 0.95 else "FAILED"
                DB.execute("INSERT INTO transfers VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL)",
                           (key, e2e, meta.get("payment_id"), meta.get("client_key"), req.get("payer"),
                            req.get("payee"), str(req.get("amount")), "PENDING", outcome, now,
                            now + random.uniform(1.0, 4.0)))
                row = DB.execute("SELECT * FROM transfers WHERE idempotency_key = ?", (key,)).fetchone()
                created = True
        if created:
            log("provider", "INFO", "transferencia aceita", idempotency_key=key, e2e_id=row["e2e_id"],
                client_key=meta.get("client_key"), amount=req.get("amount"))
            # A transferencia ja existe. A resposta e que pode demorar.
            time.sleep(2.5 if random.random() < 0.03 else random.uniform(0.05, 0.3))
        else:
            log("provider", "INFO", "chave repetida: devolvendo a transferencia existente",
                idempotency_key=key, e2e_id=row["e2e_id"])
        self._send(202 if created else 200, as_dict(row))


if __name__ == "__main__":
    threading.Thread(target=settler, daemon=True).start()
    log("provider", "INFO", "provider externo no ar", port=8090)
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
