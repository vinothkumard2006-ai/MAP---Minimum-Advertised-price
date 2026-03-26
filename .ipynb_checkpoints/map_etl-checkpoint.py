import pandas as pd
import yaml
from datetime import datetime
from sqlalchemy import create_engine

# DB CONNECTION
engine = create_engine(
    "mysql+pymysql://root:Root@123@127.0.0.1/violationdb"
)

# ---------- EXTRACT ----------
def extract():
    seller = pd.read_excel("data/Seller Mapping Table.xlsx")
    category = pd.read_json("data/Category Mapping Table.json", lines=True)
    pl = pd.read_json("data/PL Table.json", lines=True)
    sku = pd.read_xml("data/SKU Table.xml")

    with open("data/Price List Table.yaml") as f:
        price_list = pd.DataFrame(yaml.safe_load(f))

    offer = pd.read_excel("data/offertable.xlsx")
    promo = pd.read_excel("data/Promotiontable.xlsx")

    return seller, category, pl, sku, price_list, offer, promo

# ---------- TRANSFORM ----------
def get_brand(pl):
    if pl.startswith('HP'):
        return 'HP'
    elif pl.startswith('DELL'):
        return 'DELL'
    return 'Others'

def assign_season(dt):
    month = dt.month
    if month in [11, 12, 1]:
        return 'Q1(Nov-Jan)'
    elif month in [2, 3, 4]:
        return 'Q2(Feb-Apr)'
    elif month in [5, 6, 7]:
        return 'Q3(May-Jul)'
    else:
        return 'Q4(Aug-Oct)'

def calculate_promo_value(row):
    promo = str(row['offers']).lower()
    map_price = row['MAP']

    if '%' in promo:
        percent = float(promo.split('%')[0])
        return map_price * (percent / 100)

    elif '$' in promo:
        return float(promo.replace('$', '').split()[0])

    elif 'free' in promo:
        return 0

    return 0

def transform(seller, category, pl, sku, price_list, offer, promo):

    seller = seller.drop_duplicates(subset=['Ssellers_name'])

    pl = pl.drop(pl[pl['SUB_CATEGORY'].isin(
        ['Unknown', 'Single function Inkjet'])].index)

    price_list['BRAND'] = price_list['PL'].apply(get_brand)

    price_list['sku'] = price_list['sku'].astype(str).str.upper().str.strip()
    pl['sku'] = pl['sku'].astype(str).str.upper().str.strip()

    merge_table = price_list.merge(
        pl[['sku', 'Category', 'SUB_CATEGORY']],
        on='sku',
        how='left'
    ).dropna()

    merge_table['datetime'] = datetime.now()
    merge_table['SEASON_NAME'] = merge_table['datetime'].apply(assign_season)

    offer_melted = offer.melt(
        id_vars=['Category'],
        var_name='SEASON_NAME',
        value_name='offers'
    )

    promo = promo.drop(index=0)

    promo_merge = merge_table[
        merge_table['sku'].isin(promo['Promotional SKUs'])
    ]

    final_promo = promo_merge.merge(
        offer_melted[['Category', 'SEASON_NAME', 'offers']],
        on=['Category', 'SEASON_NAME'],
        how='left'
    )

    final_promo['PROMOTION_VALUE'] = final_promo.apply(
        calculate_promo_value, axis=1
    )

    tables = {
        "sku_table": sku,
        "category_mapping": pl,
        "price_list": merge_table[['PL', 'sku', 'MAP']],
        "seller_mapping": seller,
        "promotion_table": final_promo[
            ['PL', 'sku', 'SEASON_NAME', 'PROMOTION_VALUE']
        ]
    }

    return tables

# ---------- LOAD ----------
def load(tables):
    for name, df in tables.items():
        df.to_sql(name, engine, if_exists='replace', index=False)
        print(f"{name} loaded")

# ---------- MAIN ----------
def run_etl():
    data = extract()
    tables = transform(*data)
    load(tables)

if _name_ == "_main_":
    run_etl()