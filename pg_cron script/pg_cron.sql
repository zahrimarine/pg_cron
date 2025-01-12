-- Pembuatan Tabel
CREATE TABLE IF NOT EXISTS test_simple (
    id SERIAL PRIMARY KEY,
    data VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS test_medium (
    id SERIAL PRIMARY KEY,
    user_id INT,
    order_amount DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS test_complex (
    id SERIAL PRIMARY KEY,
    product_id INT,
    customer_id INT,
    quantity INT,
    price DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

-- Insert Data Awal
INSERT INTO products (name, price) VALUES
('Product A', 10.00),
('Product B', 20.00),
('Product C', 30.00)
ON CONFLICT DO NOTHING;

INSERT INTO customers (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com'),
('Charlie', 'charlie@example.com')
ON CONFLICT DO NOTHING;

-- Definisi Tugas Terjadwal
-- Tugas Sederhana
SELECT cron.schedule(
    'pgcron_simple_insert_job',
    '* * * * *',
    $$
      INSERT INTO test_simple (data) VALUES ('Simple Data');
    $$
);

-- Tugas Sedang
SELECT cron.schedule(
    'pgcron_medium_bulk_insert_job',
    '*/5 * * * *',
    $$
      INSERT INTO test_medium (user_id, order_amount)
      SELECT (random() * 1000)::INT, ROUND((random() * 100)::DECIMAL, 2)
      FROM generate_series(1, 100);
    $$
);

-- Tugas Kompleks
SELECT cron.schedule(
    'pgcron_complex_join_job',
    '0 2 * * *',
    $$
      SELECT tc.id, p.name AS product_name, c.name AS customer_name, tc.quantity, tc.price
      FROM test_complex tc
      JOIN products p ON tc.product_id = p.id
      JOIN customers c ON tc.customer_id = c.id
      WHERE tc.order_date >= NOW() - INTERVAL '1 day';
    $$
);

-- Tugas Berat dan Simultaneous Execution
SELECT cron.schedule(
    'pgcron_vacuum_job',
    '*/10 * * * *',
    $$
      VACUUM;
    $$
);

SELECT cron.schedule(
    'pgcron_analyze_job',
    '*/10 * * * *',
    $$
      ANALYZE;
    $$
);

SELECT cron.schedule(
    'pgcron_heavy_insert_job',
    '*/10 * * * *',
    $$
      INSERT INTO test_complex (product_id, customer_id, quantity, price)
      SELECT 
        (random() * (SELECT MAX(id) FROM products))::INT,
        (random() * (SELECT MAX(id) FROM customers))::INT,
        (random() * 10)::INT, 
        ROUND((random() * 100)::DECIMAL, 2)
      FROM generate_series(1, 1000);
    $$
);

SELECT cron.schedule(
    'pgcron_heavy_update_job',
    '*/10 * * * *',
    $$
      UPDATE test_complex
      SET quantity = quantity + 1
      WHERE order_date >= NOW() - INTERVAL '1 day';
    $$
);

-- Tugas yang Mengandung Error
SELECT cron.schedule(
    'pgcron_error_job',
    '*/15 * * * *',
    $$
      INSERT INTO non_existing_table (data) VALUES ('This will fail');
    $$
);

-- Retry Mechanism
CREATE OR REPLACE FUNCTION retry_insert() RETURNS void AS $$
DECLARE
    attempt INT := 0;
    max_attempts INT := 3;
BEGIN
    LOOP
        BEGIN
            INSERT INTO test_simple (data) VALUES ('Retry Insert');
            EXIT;  -- Keluar dari loop jika berhasil
        EXCEPTION WHEN others THEN
            attempt := attempt + 1;
            IF attempt >= max_attempts THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(5);  -- Tunggu 5 detik sebelum mencoba lagi
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT cron.schedule(
    'pgcron_retry_insert_job',
    '*/15 * * * *',
    $$
      SELECT retry_insert();
    $$
);
