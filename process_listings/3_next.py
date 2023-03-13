
import pandas
import pyarrow
from glob import glob

input_dir = "/data/p_dsi/capstone_projects/shea/2_deduped"

# replace listed_options ["None"] with None
df.loc[df["listed_options"].apply(lambda x: x[0]) == "None","listed_options"] = None

# replace hvf_standard and hvf_options [] with None
df.loc[df["hvf_standard"].str.len() == 0,"hvf_standard"] = None
df.loc[df["hvf_optional"].str.len() == 0,"hvf_standard"] = None

# split away unstructured features
unstructured_cols = ["seller_comments","listed_options"]
structured_cols = df.columns.remove(unstructured_cols)

# save separately
df[unstructured_cols].to_parquet()
df[structured_cols].to_parquet()