CREATE DATABASE IF NOT EXISTS DB_KNFN_OPA;

USE DB_KNFN_OPA;

CREATE TABLE IF NOT EXISTS cryptocurrency (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(10) NOT NULL,
  symbol VARCHAR(5) NOT NULL,
  creation_date DATE NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS realtime_data (
  id INT NOT NULL AUTO_INCREMENT,
  cryptocurrency_id INT NOT NULL,
  date DATE NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (cryptocurrency_id) REFERENCES cryptocurrency (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS historical_data (
  id INT NOT NULL AUTO_INCREMENT,
  cryptocurrency_id INT NOT NULL,
  time_start DATE NOT NULL,
  time_end DATE NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  open DECIMAL(10,2) NOT NULL,
  price_high DECIMAL(10,2) NOT NULL,
  volume_traded DECIMAL(20,1) NOT NULL,
  price_low DECIMAL(10,2) NOT NULL,
  price_close DECIMAL(10,2) NOT NULL,
  trades_count INT NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (cryptocurrency_id) REFERENCES cryptocurrency (id) ON DELETE CASCADE
);
