-- Dimension tables
CREATE TABLE dim_date (
    date_key        DATE PRIMARY KEY,
    day             SMALLINT,
    week            SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT
);

CREATE TABLE dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INT UNIQUE,              -- original id
    country         TEXT
);

CREATE TABLE dim_product (
    product_key     SERIAL PRIMARY KEY,
    stock_code      TEXT UNIQUE,
    description     TEXT
);

CREATE TABLE dim_invoice (
    invoice_key     SERIAL PRIMARY KEY,
    invoice_no      TEXT UNIQUE,
    is_cancelled    BOOLEAN
);

-- Fact table
CREATE TABLE fct_sales (
    sales_key       BIGSERIAL PRIMARY KEY,
    invoice_key     INT REFERENCES dim_invoice(invoice_key),
    product_key     INT REFERENCES dim_product(product_key),
    customer_key    INT REFERENCES dim_customer(customer_key),
    date_key        DATE REFERENCES dim_date(date_key),
    quantity        INT,
    unit_price      NUMERIC(10,2),
    revenue_gbp     NUMERIC(12,2)
);

CREATE INDEX idx_fct_sales_date ON fct_sales (date_key);
CREATE INDEX idx_fct_sales_invoice_key ON fct_sales (invoice_key);
CREATE INDEX idx_fct_sales_product_key ON fct_sales (product_key);
CREATE INDEX idx_fct_sales_customer_key ON fct_sales (customer_key);