
import os
import pandas as pd
import sys

state = int(sys.argv[1])
states = ["TN","TX","OH"]
state = states[state]

pickle_directory = "/data/p_dsi/capstone_projects/shea/2_deduped/" + state
pickle_files = [file for file in os.listdir(pickle_directory) if file.endswith('.pickle')]

# read in each file
dataframes = []
for file in pickle_files:
    file_path = os.path.join(pickle_directory, file)
    df = pd.read_pickle(file_path)
    dataframes.append(df)

# concatenate
combined_dataframe = pd.concat(dataframes, ignore_index=True)
df = combined_dataframe

# dedupe
mask = df['status_date'] == df.groupby('vin')['status_date'].transform(max)
df = df.loc[mask]
print(df.shape)

# replace listed_options ["None"] with None
df.loc[df["listed_options"].apply(lambda x: x[0]) == "None","listed_options"] = None

# replace hvf_standard and hvf_options [] with None
df.loc[df["hvf_standard"].str.len() == 0,"hvf_standard"] = None
df.loc[df["hvf_optional"].str.len() == 0,"hvf_optional"] = None

# split away unstructured features
unstructured_cols = ["vin","status_date","seller_comments","listed_options"]
structured_cols = df.columns.to_list()

structured_cols.remove("seller_comments")
structured_cols.remove("listed_options")

# save separately
output_dir = "/data/p_dsi/capstone_projects/shea/3_final/"
df[unstructured_cols].to_pickle(output_dir + state + "_dataset_unstructured.pkl")
df[structured_cols].to_pickle(output_dir + state + "_dataset_structured.pkl")


