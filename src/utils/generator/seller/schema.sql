DROP TABLE IF EXISTS purchases CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS resellers CASCADE;
DROP TABLE IF EXISTS organizers CASCADE;


CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW._updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';


CREATE TABLE organizers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    _created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER update_organizers_modification_time
BEFORE UPDATE ON organizers
FOR EACH ROW EXECUTE FUNCTION update_modified_column();


CREATE TABLE resellers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    commission_rate DECIMAL(4, 2) NOT NULL,
    location_region VARCHAR(100) NOT NULL,
    _created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER update_resellers_modification_time
BEFORE UPDATE ON resellers
FOR EACH ROW EXECUTE FUNCTION update_modified_column();


CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    _created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER update_customers_modification_time
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TABLE purchases (
    id SERIAL PRIMARY KEY,
    organizer_id INT NOT NULL REFERENCES organizers(id),
    reseller_id INT REFERENCES resellers(id),
    customer_id INT NOT NULL REFERENCES customers(id),
    purchase_date DATE NOT NULL,
    sales_channel VARCHAR(50) NOT NULL,
    ticket_type VARCHAR(100) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    total_amount DECIMAL(10, 2) NOT NULL,
    _created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER update_purchases_modification_time
BEFORE UPDATE ON purchases
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
