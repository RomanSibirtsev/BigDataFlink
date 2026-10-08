CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key UUID PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  age INTEGER,
  email TEXT,
  country TEXT,
  postal_code TEXT,
  pet_type TEXT,
  pet_name TEXT,
  pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
  seller_key UUID PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  country TEXT,
  postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
  supplier_key UUID PRIMARY KEY,
  name TEXT,
  contact TEXT,
  email TEXT,
  phone TEXT,
  address TEXT,
  city TEXT,
  country TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_key UUID PRIMARY KEY,
  name TEXT,
  category TEXT,
  price NUMERIC(12, 2),
  stock_quantity INTEGER,
  pet_category TEXT,
  weight NUMERIC(12, 3),
  color TEXT,
  size TEXT,
  brand TEXT,
  material TEXT,
  description TEXT,
  rating NUMERIC(3, 1),
  reviews INTEGER,
  release_date DATE,
  expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
  store_key UUID PRIMARY KEY,
  name TEXT,
  location TEXT,
  city TEXT,
  state TEXT,
  country TEXT,
  phone TEXT,
  email TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_key DATE PRIMARY KEY,
  day SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL,
  year SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
  sale_key UUID PRIMARY KEY,
  customer_key UUID NOT NULL REFERENCES dim_customer(customer_key),
  seller_key UUID NOT NULL REFERENCES dim_seller(seller_key),
  product_key UUID NOT NULL REFERENCES dim_product(product_key),
  store_key UUID NOT NULL REFERENCES dim_store(store_key),
  supplier_key UUID NOT NULL REFERENCES dim_supplier(supplier_key),
  date_key DATE NOT NULL REFERENCES dim_date(date_key),
  quantity INTEGER,
  total_price NUMERIC(12, 2),
  source_file TEXT NOT NULL,
  source_row BIGINT NOT NULL,
  UNIQUE (source_file, source_row)
);

CREATE INDEX IF NOT EXISTS fact_sales_date_idx ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS fact_sales_product_idx ON fact_sales(product_key);