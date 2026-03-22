-- TPC-C inspired schema for DeltaForge CDC soak testing.
--
-- 9 tables modelling a wholesale supplier (warehouses, districts, customers,
-- orders, order-lines, items, stock, history).  All data lives in the
-- `orders` schema so the chaos DeltaForge pipeline picks it up automatically.
--
-- Type coverage exercised across the 9 tables:
--   INT, SMALLINT, TINYINT (signed)  |  DECIMAL(4,4) / (5,2) / (6,2) / (8,2) / (12,2)
--   CHAR(2) / (9) / (16) / (24)     |  VARCHAR(10) / (16) / (20) / (24) / (50) / (500)
--   DATETIME                         |  composite PRIMARY KEYs + secondary indexes
--
-- The seed-data rows (warehouse, district, item, stock, customer) are inserted
-- by the Rust scenario runner at startup -- these CREATE statements only
-- define the schema.

USE orders;

-- ── warehouse ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tpcc_warehouse (
  w_id       SMALLINT     NOT NULL,
  w_name     VARCHAR(10)  NOT NULL DEFAULT '',
  w_street_1 VARCHAR(20)  NOT NULL DEFAULT '',
  w_street_2 VARCHAR(20)  NOT NULL DEFAULT '',
  w_city     VARCHAR(20)  NOT NULL DEFAULT '',
  w_state    CHAR(2)      NOT NULL DEFAULT '',
  w_zip      CHAR(9)      NOT NULL DEFAULT '',
  w_tax      DECIMAL(4,4) NOT NULL DEFAULT 0.0000,
  w_ytd      DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  PRIMARY KEY (w_id)
);

-- ── district (10 per warehouse) ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tpcc_district (
  d_id       TINYINT      NOT NULL,
  d_w_id     SMALLINT     NOT NULL,
  d_name     VARCHAR(10)  NOT NULL DEFAULT '',
  d_street_1 VARCHAR(20)  NOT NULL DEFAULT '',
  d_street_2 VARCHAR(20)  NOT NULL DEFAULT '',
  d_city     VARCHAR(20)  NOT NULL DEFAULT '',
  d_state    CHAR(2)      NOT NULL DEFAULT '',
  d_zip      CHAR(9)      NOT NULL DEFAULT '',
  d_tax      DECIMAL(4,4) NOT NULL DEFAULT 0.0000,
  d_ytd      DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  d_next_o_id INT         NOT NULL DEFAULT 1,
  PRIMARY KEY (d_w_id, d_id)
);

-- ── customer (CUSTOMERS_PER_DISTRICT per district) ────────────────────────────
-- Heaviest seed table; dominant target for Payment UPDATE events.
CREATE TABLE IF NOT EXISTS tpcc_customer (
  c_id          INT          NOT NULL,
  c_d_id        TINYINT      NOT NULL,
  c_w_id        SMALLINT     NOT NULL,
  c_first       VARCHAR(16)  NOT NULL DEFAULT '',
  c_middle      CHAR(2)      NOT NULL DEFAULT 'OE',
  c_last        VARCHAR(16)  NOT NULL DEFAULT '',
  c_street_1    VARCHAR(20)  NOT NULL DEFAULT '',
  c_street_2    VARCHAR(20)  NOT NULL DEFAULT '',
  c_city        VARCHAR(20)  NOT NULL DEFAULT '',
  c_state       CHAR(2)      NOT NULL DEFAULT '',
  c_zip         CHAR(9)      NOT NULL DEFAULT '',
  c_phone       CHAR(16)     NOT NULL DEFAULT '',
  c_since       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  c_credit      CHAR(2)      NOT NULL DEFAULT 'GC',
  c_credit_lim  DECIMAL(12,2) NOT NULL DEFAULT 50000.00,
  c_discount    DECIMAL(4,4) NOT NULL DEFAULT 0.0000,
  c_balance     DECIMAL(12,2) NOT NULL DEFAULT -10.00,
  c_ytd_payment DECIMAL(12,2) NOT NULL DEFAULT 10.00,
  c_payment_cnt SMALLINT     NOT NULL DEFAULT 1,
  c_delivery_cnt SMALLINT    NOT NULL DEFAULT 0,
  c_data        VARCHAR(500) NOT NULL DEFAULT '',
  PRIMARY KEY (c_w_id, c_d_id, c_id),
  INDEX idx_c_last (c_last, c_first)
);

-- ── history (insert-only; one row per Payment transaction) ────────────────────
CREATE TABLE IF NOT EXISTS tpcc_history (
  h_c_id   INT          NOT NULL,
  h_c_d_id TINYINT      NOT NULL,
  h_c_w_id SMALLINT     NOT NULL,
  h_d_id   TINYINT      NOT NULL,
  h_w_id   SMALLINT     NOT NULL,
  h_date   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  h_amount DECIMAL(6,2) NOT NULL DEFAULT 0.00,
  h_data   VARCHAR(24)  NOT NULL DEFAULT '',
  INDEX idx_h_cust (h_c_w_id, h_c_d_id, h_c_id)
);

-- ── item (global catalog; read-only in standard TPC-C, UPDATEd by alter loop) ─
CREATE TABLE IF NOT EXISTS tpcc_item (
  i_id    INT          NOT NULL,
  i_im_id INT          NOT NULL DEFAULT 0,
  i_name  VARCHAR(24)  NOT NULL DEFAULT '',
  i_price DECIMAL(5,2) NOT NULL DEFAULT 0.00,
  i_data  VARCHAR(50)  NOT NULL DEFAULT '',
  PRIMARY KEY (i_id)
);

-- ── stock (per-warehouse inventory; hot UPDATE target in New-Order) ───────────
CREATE TABLE IF NOT EXISTS tpcc_stock (
  s_i_id        INT          NOT NULL,
  s_w_id        SMALLINT     NOT NULL,
  s_quantity    SMALLINT     NOT NULL DEFAULT 0,
  s_dist_01     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_02     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_03     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_04     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_05     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_06     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_07     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_08     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_09     CHAR(24)     NOT NULL DEFAULT '',
  s_dist_10     CHAR(24)     NOT NULL DEFAULT '',
  s_ytd         DECIMAL(8,2) NOT NULL DEFAULT 0.00,
  s_order_cnt   SMALLINT     NOT NULL DEFAULT 0,
  s_remote_cnt  SMALLINT     NOT NULL DEFAULT 0,
  s_data        VARCHAR(50)  NOT NULL DEFAULT '',
  PRIMARY KEY (s_w_id, s_i_id)
);

-- ── order ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tpcc_order (
  o_id         INT      NOT NULL,
  o_d_id       TINYINT  NOT NULL,
  o_w_id       SMALLINT NOT NULL,
  o_c_id       INT      NOT NULL DEFAULT 0,
  o_entry_d    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  o_carrier_id TINYINT  DEFAULT NULL,
  o_ol_cnt     TINYINT  NOT NULL DEFAULT 0,
  o_all_local  TINYINT  NOT NULL DEFAULT 1,
  PRIMARY KEY (o_w_id, o_d_id, o_id),
  INDEX idx_o_cust (o_w_id, o_d_id, o_c_id)
);

-- ── new_order (pending-delivery subset of order; rows deleted by Delivery) ────
CREATE TABLE IF NOT EXISTS tpcc_new_order (
  no_o_id INT      NOT NULL,
  no_d_id TINYINT  NOT NULL,
  no_w_id SMALLINT NOT NULL,
  PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

-- ── order_line (5-15 rows per order; hot INSERT + UPDATE target) ──────────────
CREATE TABLE IF NOT EXISTS tpcc_order_line (
  ol_o_id        INT          NOT NULL,
  ol_d_id        TINYINT      NOT NULL,
  ol_w_id        SMALLINT     NOT NULL,
  ol_number      TINYINT      NOT NULL,
  ol_i_id        INT          NOT NULL DEFAULT 0,
  ol_supply_w_id SMALLINT     NOT NULL DEFAULT 0,
  ol_delivery_d  DATETIME     DEFAULT NULL,
  ol_quantity    SMALLINT     NOT NULL DEFAULT 0,
  ol_amount      DECIMAL(6,2) NOT NULL DEFAULT 0.00,
  ol_dist_info   CHAR(24)     NOT NULL DEFAULT '',
  PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);
