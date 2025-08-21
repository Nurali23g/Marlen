import pandas as pd
from datetime import datetime
import os

def truncate_string(value, length):
    if pd.isna(value):
        return None
    return str(value)[:length]

def normalize_category(raw_category: str) -> str:
    """
    Takes 'Продавцы_в_категории_Компьютеры' and returns 'computers'.
    """
    ru_cat = raw_category.replace("Продавцы_в_категории_", "").strip().lower()

    mapping = {
        "тв,_аудио,_видео": "TV_Audio",
        "телефоны_и_гаджеты": "Smartphones and gadgets",
        "товары_для_дома_и_дачи": "Home",
        "бытовая_техника": "Home equipment",
        "детские_товары": "Child goods",
        "компьютеры": "Computers",
        "красота_и_здоровье": "Beauty care",
        "мебель": "Furniture"
    }
    return mapping.get(ru_cat, ru_cat)

def excel_to_csv(input_excel, category_eng, parse_date):
    df = pd.read_excel(input_excel, skiprows=4)

    # Normalize columns
    df.rename(columns={
        "ID магазина": "merchant_id",
        "Наименование": "name",
        "Кол-во артикулов": "count_articuls",
        "Кол-во брендов": "count_brand",
        "Успешных продаж": "popularity",
        "Продаж в день": "sales_1day",
        "Мин. цена магазина": "min_price",
        "Макс. цена магазина": "max_price",
        "Рейтинг": "rating",
        "Дата регистрации": "create_date"
    }, inplace=True)

    df_out = pd.DataFrame({
        "id": [truncate_string(f"{m}_{category_eng}", 100) if pd.notna(m) else None
               for m in df["merchant_id"]],
        "merchant_id": df["merchant_id"].astype(str).where(df["merchant_id"].notna(), None),
        "title": df["name"].where(df["name"].notna(), None),
        "name": df["name"].where(df["name"].notna(), None),
        "count": None,
        "popularity": df["popularity"].where(df["popularity"].notna(), None),
        "category": category_eng,
        "parse_date": parse_date,
        "is_new_data": True,
        "count_articuls": df["count_articuls"].where(df["count_articuls"].notna(), None),
        "count_brand": df["count_brand"].where(df["count_brand"].notna(), None),
        "sales_1day": df["sales_1day"].where(df["sales_1day"].notna(), None),
        "min_price": df["min_price"].where(df["min_price"].notna(), None),
        "max_price": df["max_price"].where(df["max_price"].notna(), None),
        "create_date": df["create_date"].where(df["create_date"].notna(), None),
    })

    return df_out

def process_folder(input_folder, output_csv):
    parse_date = datetime.now().strftime("%Y-%m-%d")
    all_data = []

    for file in os.listdir(input_folder):
        if file.endswith(".xlsx"):
            file_path = os.path.join(input_folder, file)

            metadata = pd.read_excel(file_path, nrows=1, header=None)
            raw_title = str(metadata.iloc[0, 1])
            category_eng = normalize_category(raw_title)

            print(f"📂 Processing {file} → category: {category_eng}")
            df_out = excel_to_csv(file_path, category_eng, parse_date)
            all_data.append(df_out)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_csv, sep="|", index=False, encoding="utf-8")
        print(f"✅ Saved all data to {output_csv}")
    else:
        print("⚠️ No Excel files found in folder!")

if __name__ == "__main__":
    
    input_folder = "excel"          # путь к папке с ексель

    output_csv = "kaspi_merchants_from_all_excels.csv" 
    process_folder(input_folder, output_csv)
