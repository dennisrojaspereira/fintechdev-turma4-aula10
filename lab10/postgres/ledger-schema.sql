-- Banco do Ledger (owner: Ledger Service).

CREATE TABLE ledger_entries (
    id         bigserial PRIMARY KEY,
    payment_id uuid NOT NULL,
    account    text NOT NULL,
    direction  text NOT NULL,                     -- DEBIT | CREDIT
    amount     numeric(12, 2) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_by_payment ON ledger_entries (payment_id);

-- Idempotencia do consumidor: o evento so produz efeito uma vez.
CREATE TABLE processed_events (
    event_id     text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
