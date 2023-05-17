CREATE TABLE IF NOT EXISTS "utxos" (
  "id"             integer     PRIMARY KEY,
  "txid"           text        NOT NULL,
  "vout"           integer     NOT NULL,
  "address"        text        NOT NULL REFERENCES "address" ("id"),
  "value"          integer     NOT NULL,
  "date"           integer     NOT NULL,

  UNIQUE ("txid", "vout")
) STRICT;

CREATE TABLE IF NOT EXISTS "addresses" (
  "id"          text             PRIMARY KEY,
  "index"       integer          NOT NULL UNIQUE,
  "label"       text             DEFAULT NULL,
  "received"    integer          NOT NULL DEFAULT 0,
  "used"        integer          NOT NULL DEFAULT false
) STRICT;

CREATE TABLE IF NOT EXISTS "sync" (
  "network"     text             NOT NULL UNIQUE,
  "height"      integer          NOT NULL UNIQUE,
  "hash"        text             NOT NULL UNIQUE
) STRICT;
