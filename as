alter table
add column fff boolean not null default False


CREATE TABLE fgp_dsx_dm.kaspi_merchants (
	id varchar(100) NULL,
	merchant_id varchar(50) NULL,
	title varchar(100) NULL,
	"name" varchar(100) NULL,
	count numeric NULL,
	popularity numeric NULL,
	category varchar(50) NULL,
	parse_date date NULL
)
WITH (
	appendonly=true,
	compresstype=zstd,
	compresslevel=3
)
DISTRIBUTED BY (id);
