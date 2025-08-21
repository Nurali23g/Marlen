ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN brand varchar(100),
ADD COLUMN articul varchar(100),
ADD COLUMN sales_per_day numeric,
ADD COLUMN price numeric,
ADD COLUMN rating numeric,
ADD COLUMN reviews_count numeric,
ADD COLUMN is_new_data boolean NOT NULL DEFAULT false;
