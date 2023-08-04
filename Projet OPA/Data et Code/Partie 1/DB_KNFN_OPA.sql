CREATE TABLE IF NOT EXISTS cryptocurrency (
  id SERIAL NOT NULL,
  name VARCHAR(10) NOT NULL,
  symbol VARCHAR(5) NOT NULL,
  creation_date DATE NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS realtime_data (
  id SERIAL NOT NULL,
  cryptocurrency_id INT NOT NULL,
  date TIMESTAMPTZ NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (id, date),
  FOREIGN KEY (cryptocurrency_id) REFERENCES cryptocurrency (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS historical_data (
  id SERIAL NOT NULL,
  cryptocurrency_id INT NOT NULL,
  time_start TIMESTAMPTZ NOT NULL,
  time_end TIMESTAMPTZ NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  open DECIMAL(10,2) NOT NULL,
  price_high DECIMAL(10,2) NOT NULL,
  volume_traded DECIMAL(20,1) NOT NULL,
  price_low DECIMAL(10,2) NOT NULL,
  price_close DECIMAL(10,2) NOT NULL,
  trades_count INT NOT NULL,
  PRIMARY KEY (id, time_start),
  FOREIGN KEY (cryptocurrency_id) REFERENCES cryptocurrency (id) ON DELETE CASCADE
);

CREATE INDEX ON realtime_data(date DESC);
CREATE INDEX ON historical_data(time_start DESC);

SELECT create_hypertable('historical_data', 'time_start');
SELECT create_hypertable('realtime_data', 'date');
