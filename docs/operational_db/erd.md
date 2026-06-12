# Entity Relationship Diagram — Operational Database

> Database: `event_platform` (PostgreSQL)  
> All tables carry `_created_at` and `_updated_at` audit timestamps managed by the `set_updated_at()` trigger function.

```mermaid
erDiagram
    ORGANIZER {
        uuid organizer_id PK
        varchar name "NOT NULL"
        varchar email "UNIQUE NOT NULL"
        varchar phone "NULL"
        text address "NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    RESELLER {
        uuid reseller_id PK
        varchar name "NOT NULL"
        varchar email "UNIQUE NOT NULL"
        varchar phone "NULL"
        varchar city "NULL"
        varchar country "NULL"
        varchar region "NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    COMMISSION_AGREEMENT {
        uuid agreement_id PK
        uuid organizer_id FK "NOT NULL"
        uuid reseller_id FK "NOT NULL"
        numeric commission_rate "NOT NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    CUSTOMER {
        uuid customer_id PK
        varchar first_name "NULL"
        varchar last_name "NULL"
        varchar email "UNIQUE NOT NULL"
        varchar phone "NULL"
        varchar region "NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    EVENT {
        uuid event_id PK
        uuid organizer_id FK "NOT NULL"
        varchar name "NOT NULL"
        varchar type "NULL"
        varchar venue "NULL"
        varchar region "NULL"
        date event_date "NOT NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    TICKET {
        uuid ticket_id PK
        uuid event_id FK "NOT NULL"
        varchar category "NOT NULL"
        numeric face_value "NOT NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    SALE {
        uuid sale_id PK
        uuid ticket_id FK "NOT NULL"
        uuid customer_id FK "NOT NULL"
        uuid organizer_id FK "NOT NULL"
        uuid reseller_id FK "NULL"
        uuid agreement_id FK "NULL"
        varchar channel "NOT NULL"
        integer quantity "NOT NULL"
        numeric unit_price "NOT NULL"
        numeric total_amount "NOT NULL"
        numeric commission_amount "NOT NULL DEFAULT 0.00"
        varchar source "NOT NULL"
        date sale_date "NOT NULL"
        timestamp _created_at "NOT NULL DEFAULT NOW()"
        timestamp _updated_at "NOT NULL DEFAULT NOW()"
    }

    ORGANIZER ||--o{ COMMISSION_AGREEMENT : "signs"
    RESELLER ||--o{ COMMISSION_AGREEMENT : "signs"
    ORGANIZER ||--o{ EVENT : "manages"
    EVENT ||--o{ TICKET : "provisions"
    CUSTOMER ||--o{ SALE : "purchases"
    TICKET ||--o{ SALE : "includes"
    
    %% Architectural Dimension Denormalizations for Ingestion Paths
    ORGANIZER ||--o{ SALE : "allocates revenue (denormalized)"
    RESELLER ||--o{ SALE : "attributes execution (denormalized)"
    COMMISSION_AGREEMENT ||--o{ SALE : "applies rates (denormalized)"
```

---

## Relationship Summary

| Relationship | Type | Description |
|---|---|---|
| `ORGANIZER` → `EVENT` | One-to-many | An organizer runs zero or more events |
| `ORGANIZER` → `COMMISSION_AGREEMENT` | One-to-many | An organizer may have agreements with multiple resellers |
| `RESELLER` → `COMMISSION_AGREEMENT` | One-to-many | A reseller may have agreements with multiple organizers |
| `EVENT` → `TICKET` | One-to-many | An event offers one or more ticket categories |
| `TICKET` → `SALE` | One-to-many | A ticket type can be sold multiple times |
| `CUSTOMER` → `SALE` | One-to-many | A customer can make multiple purchases |
| `ORGANIZER` → `SALE` | One-to-many | Denormalized reference — identifies which organizer's event was sold |
| `RESELLER` → `SALE` | One-to-many | Denormalized reference — identifies the reseller who facilitated the sale |
| `COMMISSION_AGREEMENT` → `SALE` | One-to-many | Links the applicable commission agreement to each reseller sale |
