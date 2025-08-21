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




CREATE TABLE fgp_de_sandbox.kaspi_merchants_categories (
	id varchar(100) NULL,
	parent_id varchar(100) NULL,
	title varchar(100) NULL,
	titleru varchar(100) NULL,
	link varchar(100) NULL,
	"active" bool NULL,
	count numeric NULL,
	popularity numeric NULL,
	expanded bool NULL,
	"level" numeric NULL
)
WITH (
	appendonly=true,
	compresstype=zstd,
	compresslevel=3
)
DISTRIBUTED BY (id);
