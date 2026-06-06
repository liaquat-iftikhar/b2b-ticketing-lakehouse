DROP TABLE IF EXISTS organizer CASCADE;
DROP TABLE IF EXISTS reseller CASCADE;
DROP TABLE IF EXISTS commission_agreement CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS event CASCADE;
DROP TABLE IF EXISTS ticket CASCADE;
DROP TABLE IF EXISTS sale CASCADE;


CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW._updated_at = NOW();
  RETURN NEW;
END;


CREATE TABLE organizer (
    organizer_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    address TEXT,
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()

);

CREATE TRIGGER trg_organizer_updated_at
  BEFORE UPDATE ON organizer
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE reseller (
    reseller_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(50),
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_reseller_updated_at
  BEFORE UPDATE ON reseller
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


CREATE TABLE commission_agreement (
    agreement_id UUID PRIMARY KEY,
    organizer_id UUID REFERENCES organizer(organizer_id),
    reseller_id UUID REFERENCES reseller(reseller_id),
    commission_rate DECIMAL(5,2) NOT NULL,
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_commission_agreement_updated_at
  BEFORE UPDATE ON commission_agreement
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


CREATE TABLE customer (
    customer_id UUID PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    region VARCHAR(50),
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_customer_updated_at
  BEFORE UPDATE ON customer
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE event (
    event_id UUID PRIMARY KEY,
    organizer_id UUID REFERENCES organizer(organizer_id),
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100),
    venue VARCHAR(255),
    region VARCHAR(50),
    event_date DATE,
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_event_updated_at
  BEFORE UPDATE ON event
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


CREATE TABLE ticket (
    ticket_id UUID PRIMARY KEY,
    event_id UUID REFERENCES event(event_id),
    category VARCHAR(100),
    face_value DECIMAL(10,2) NOT NULL,
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TRIGGER trg_ticket_updated_at
  BEFORE UPDATE ON ticket
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE sale (
    sale_id UUID PRIMARY KEY,
    ticket_id UUID REFERENCES ticket(ticket_id),
    customer_id UUID REFERENCES customer(customer_id),
    organizer_id UUID REFERENCES organizer(organizer_id),
    reseller_id UUID REFERENCES reseller(reseller_id),
    agreement_id UUID REFERENCES commission_agreement(agreement_id),
    channel VARCHAR(50),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    commission_amount DECIMAL(10,2) DEFAULT 0.00,
    source VARCHAR(50),
    sale_date DATE,
    _created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    _updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_sale_updated_at
  BEFORE UPDATE ON sale
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
