-- Banco do Payment (owner: Payment Service).

CREATE TABLE payments (
    id           uuid PRIMARY KEY,                -- gerado pela Tech Pix; vai ao provider como chave de idempotencia
    client_key   text NOT NULL UNIQUE,            -- Idempotency-Key enviada pelo cliente
    payer        text NOT NULL,
    payee        text NOT NULL,
    amount       numeric(12, 2) NOT NULL,
    status       text NOT NULL,                   -- PROCESSING | UNKNOWN | COMPLETED | FAILED
    provider_ref text,                            -- e2e_id devolvido pelo provider
    origin       text NOT NULL DEFAULT 'api',     -- api | reconciliation
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_open ON payments (status) WHERE status IN ('PROCESSING', 'UNKNOWN');

-- Transactional Outbox: o evento nasce na mesma transacao do estado.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    event_id     text NOT NULL UNIQUE,            -- deterministico: <payment_id>:PaymentCompleted
    event_type   text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;

-- Operacoes ambiguas ou duplicadas: ficam aqui ate uma decisao humana.
CREATE TABLE quarantine (
    client_key text NOT NULL,
    reason     text NOT NULL,
    details    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_key, reason)
);
