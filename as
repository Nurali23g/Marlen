CREATE EXTERNAL TABLE gpdb.fgp_dsx_ext.ext_ozon_merchants (
	supplier_id numeric,
	text varchar,
	bin varchar
)
LOCATION (
	'gpfdist://10.0.84.192:8088/ozon_merchants.csv'
) ON ALL
FORMAT 'CSV' ( delimiter '|' null '' escape '\' quote '"' )
ENCODING 'UTF8';









ALTER TABLE fgp_dsx_dm.kaspi_merchants
ADD COLUMN brand varchar(100),
ADD COLUMN articul varchar(100),
ADD COLUMN sales_per_day numeric,
ADD COLUMN price numeric,
ADD COLUMN rating numeric,
ADD COLUMN reviews_count numeric,
ADD COLUMN is_new_data boolean NOT NULL DEFAULT false;


ТВ,_Аудио,_Видео  TV_Audio
Телефоны_и_гаджеты  Smartphones and gadgets
Товары_для_дома_и_дачи  Home
Бытовая_техника Home equipment
Детские_товары  Child goods
Компьютеры  Computers
Красота_и_здоровье  Beauty care
Мебель  Furniture


CREATE TABLE fgp_dsx_dm.kaspi_merchants_detailed (
	merchant_id varchar(50) NULL,
	"name" varchar(100) NULL,
	phone varchar(20) NULL,
	create_date varchar(50) NULL,
	salescount numeric NULL,
	rating numeric NULL,
	ratingcount numeric NULL,
	reviewscount numeric NULL,
	parse_date date NULL
)
WITH (
	appendonly=true,
	compresstype=zstd,
	compresslevel=3
)
DISTRIBUTED BY (merchant_id);
