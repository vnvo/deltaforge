-- init-db.sql (for dev)
-- This script runs automatically when the MySQL container starts for the first time

-- Create multiple databases
CREATE DATABASE IF NOT EXISTS orders;
CREATE DATABASE IF NOT EXISTS inventory;
CREATE DATABASE IF NOT EXISTS users;
CREATE DATABASE IF NOT EXISTS analytics;

-- Create a user for your application
CREATE USER IF NOT EXISTS 'df'@'%' IDENTIFIED BY 'dfpw';

-- Grant privileges on all databases
GRANT ALL PRIVILEGES ON orders.* TO 'df'@'%';
GRANT ALL PRIVILEGES ON inventory.* TO 'df'@'%';
GRANT ALL PRIVILEGES ON users.* TO 'df'@'%';
GRANT ALL PRIVILEGES ON analytics.* TO 'df'@'%';

-- Grant replication privileges (needed for binlog reading)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'df'@'%';

-- Apply changes
FLUSH PRIVILEGES;

-- Optional: Create some sample tables
USE orders;
CREATE TABLE IF NOT EXISTS order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(100),
    product_name VARCHAR(255),
    quantity INT,
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

USE inventory;
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(100) UNIQUE,
    name VARCHAR(255),
    stock_quantity INT,
    price DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

USE users;
CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);