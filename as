ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN articul varchar(100);

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN brand varchar(100);

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN sales_per_day numeric;

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN price numeric;

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN rating numeric;

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN reviews_count numeric;

ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN is_new_data boolean NOT NULL DEFAULT false;
