


CREATE TABLE IF NOT EXISTS address
(
    id integer,
    company_id text COLLATE pg_catalog."default",
    country character varying(255) COLLATE pg_catalog."default",
    postal_code character varying(255) COLLATE pg_catalog."default",
    city character varying(255) COLLATE pg_catalog."default",
    district character varying(255) COLLATE pg_catalog."default",
    street character varying(255) COLLATE pg_catalog."default",
    street_number character varying(255) COLLATE pg_catalog."default",
    addition character varying(255) COLLATE pg_catalog."default",
    created_at date
);

CREATE TABLE IF NOT EXISTS company
(
    company_id text COLLATE pg_catalog."default",
    status character varying(255) COLLATE pg_catalog."default",
    rating_threshold character varying(255) COLLATE pg_catalog."default",
    company_name text COLLATE pg_catalog."default",
    foundation_date date,
    legal_form character varying(255) COLLATE pg_catalog."default",
    created_at date
);

CREATE TABLE IF NOT EXISTS last_sync_date
(
    synced_at TIMESTAMP DEFAULT 0
);
