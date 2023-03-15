
import pandas as pd
import pyarrow

def keep_latest(df, dedupe_by_cols, sort_by_cols):
    """Keep last record for each unique combination of dupe_cols, ordering by sort_cols"""
    df = df.sort_values(sort_by_cols).drop_duplicates(dedupe_by_cols, keep="last")
    return df

registration_dir = "/data/p_dsi/capstone_projects/shea/registrations/"

############
# listings
############

# dir path
listings_dir = "/data/p_dsi/capstone_projects/shea/3_final"

# construct list of parquet files
parquet_files = [
    os.path.join(parquet_dir, f)
    for f in os.listdir(parquet_dir)
    if f.endswith(".pkl")
]

# read into pandas dataframe
listings_df = pd.concat([pd.read_parquet(f, columns=load_cols) for f in parquet_files])
print_shape(listings_df)


############
# texas
############

tx_file = registration_dir + "tx_mvr_out.parquet"

registrations_tx = fp.ParquetFile(tx_file).to_pandas(
    columns=["VIN", "SALE_DATE", "SALES_PRICE"]
)

# rename columns
registrations_tx = registrations_tx.rename(columns={'VIN':'vin','SALE_DATE':'mvr_purchase_date','SALES_PRICE':'mvr_price'})

# convert to date type
registrations_tx['mvr_purchase_date'] = pd.to_datetime(registrations_tx['mvr_purchase_date'],format="\'%Y-%m-%d\'")


# dedupe vin by date
registrations_tx = keep_latest(registrations_tx, ["vin"], ["mvr_purchase_date"])
print_shape(registrations_tx)


############
# ohio
############

oh_file = registration_dir + "oh_mvr_out.parquet"

registrations_oh = fp.ParquetFile(oh_file).to_pandas(
    columns=['VIN','PurchaseDate','PurchasePrice']
)

# rename columns
registrations_oh = registrations_oh.rename(columns={'VIN':'vin','PurchaseDate':'mvr_purchase_date','PurchasePrice':'mvr_price'})

# convert to date type
registrations_oh['mvr_purchase_date'] = pd.to_datetime(registrations_oh['mvr_purchase_date'],format="%Y-%m-%d")

# dedupe vin by date
registrations_oh = keep_latest(registrations_oh, ["vin"], ["mvr_purchase_date"])
print_shape(registrations_oh)


############
# tennessee
############

tn_file = registration_dir + "tn_mvr.parquet"

registrations_tn = fp.ParquetFile(tn_file).to_pandas(
    columns=['vin','purchase_date','price']
)

# rename columns
registrations_tn = registrations_tn.rename(columns={'vin':'vin','purchase_date':'mvr_purchase_date','price':'mvr_price'})

# convert to date type
registrations_tn['mvr_purchase_date'] = pd.to_datetime(registrations_tn['mvr_purchase_date'],format="%Y-%m-%d", errors="coerce")

# dedupe vin by date
registrations_tn = keep_latest(registrations_tn, ["vin"], ["mvr_purchase_date"])

############
# merge
############

# stack all registrations
registrations = pd.concat([registrations_tx, registrations_oh, registrations_tn])

# merge with listings
df = listings_df.merge(registrations, on='vin', how='inner')

# match rate
round(df["vin"].nunique()/listings_df["vin"].nunique(),2)

# write to parquet, excluding unstructured features
output_dir = "/data/p_dsi/capstone_projects/shea/4_merged/"
df.to_parquet(output_dir + "merged.parquet")


