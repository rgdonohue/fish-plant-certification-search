# %%
import pandas as pd

# %%
plants_original_df = pd.read_csv('../data/plants_to_scrape_for_certs_original_01-03.csv')
plants_with_info_df = pd.read_csv('../data/plants_with_info_certs_updated.csv')
plants_without_info_df = pd.read_csv('../data/plants_without_info_certs_updated.csv')

# %%


# %%
# Create a copy of the original DataFrame to avoid modifying it
updated_plants_df = plants_original_df.copy()

# Combine plants_with_info_df and plants_without_info_df, dropping duplicates based on Company name
all_updated_info = pd.concat([plants_with_info_df, plants_without_info_df]).drop_duplicates(subset=['Company name'])

# For each row in the original DataFrame
for index, row in updated_plants_df.iterrows():
    # Get matching row from combined updated info based on some identifier (assuming index matches)
    updated_row = all_updated_info.loc[all_updated_info.index == index]
    
    if not updated_row.empty:
        # For each column in the DataFrame
        for column in updated_plants_df.columns:
            # If the value is NA/null in original, update it with the new value
            if pd.isna(row[column]) and not pd.isna(updated_row[column].iloc[0]):
                updated_plants_df.at[index, column] = updated_row[column].iloc[0]

# Write the updated DataFrame to a new CSV file
updated_plants_df.to_csv('../data/plants_updated_certification_and_info.csv', index=False)


# %%
