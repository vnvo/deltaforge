-- Chaos test database setup
-- Creates the CDC user and seed tables used by chaos scenarios.

CREATE DATABASE IF NOT EXISTS orders;
USE orders;

CREATE TABLE IF NOT EXISTS customers (
  id       INT AUTO_INCREMENT PRIMARY KEY,
  name     VARCHAR(255) NOT NULL,
  email    VARCHAR(255) UNIQUE,
  balance  DECIMAL(10,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_events (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount     DECIMAL(10,2) NOT NULL,
  status     VARCHAR(50) DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email, balance) VALUES
  ('Alice',   'alice@example.com',   1000.00),
  ('Bob',     'bob@example.com',     500.00),
  ('Charlie', 'charlie@example.com', 250.00);

-- CDC replication user
CREATE USER IF NOT EXISTS 'cdc_user'@'%' IDENTIFIED WITH mysql_native_password BY 'cdc_password';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;