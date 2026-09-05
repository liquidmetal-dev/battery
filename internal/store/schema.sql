CREATE TABLE IF NOT EXISTS pools (
    name                          TEXT NOT NULL,
    namespace                     TEXT NOT NULL,
    size                          INTEGER NOT NULL,
    flintlock_hosts               TEXT NOT NULL,
    microvm_template              TEXT NOT NULL,
    replenishment_strategy        TEXT NOT NULL,
    create_commands               TEXT NOT NULL,
    pre_lease_commands            TEXT NOT NULL,
    hook_failure_policy           INTEGER NOT NULL,
    heartbeat_interval_ns         INTEGER NOT NULL,
    heartbeat_expiry_threshold_ns INTEGER NOT NULL,
    PRIMARY KEY (name, namespace)
);

CREATE TABLE IF NOT EXISTS vms (
    uid            TEXT PRIMARY KEY,
    pool_name      TEXT NOT NULL,
    pool_namespace TEXT NOT NULL,
    flintlock_host TEXT NOT NULL,
    phase          INTEGER NOT NULL,
    lease_id       TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vms_pool_phase ON vms (pool_namespace, pool_name, phase);

CREATE TABLE IF NOT EXISTS leases (
    lease_id           TEXT PRIMARY KEY,
    vm_uid             TEXT NOT NULL,
    pool_name          TEXT NOT NULL,
    pool_namespace     TEXT NOT NULL,
    claimed_at         INTEGER NOT NULL,
    last_heartbeat_at  INTEGER NOT NULL,
    expires_at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_expires_at ON leases (expires_at);

CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_name      TEXT NOT NULL,
    pool_namespace TEXT NOT NULL,
    vm_uid         TEXT NOT NULL,
    type           INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    payload_json   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_pool_id ON events (pool_namespace, pool_name, id);
