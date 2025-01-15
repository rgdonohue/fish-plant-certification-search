# %%
import pandas as pd

# %%
plants_original_df = pd.read_csv('../data/plants_to_scrape_for_certs_original_01-03.csv')
plants_with_info_df = pd.read_csv('../data/plants_with_info_certs_updated.csv')
plants_without_info_df = pd.read_csv('../data/plants_without_info_certs_updated.csv')

# %%
# First, combine the two info dataframes
combined_info_df = pd.concat([plants_with_info_df, plants_without_info_df], ignore_index=True)
# Remove duplicates from combined info before merging
combined_info_df = combined_info_df.drop_duplicates()

# Add any missing rows from original df to combined_info_df
missing_rows = plants_original_df[~plants_original_df['Company name'].isin(combined_info_df['Company name'])]
combined_info_df = pd.concat([combined_info_df, missing_rows], ignore_index=True)

# Remove any duplicates and save
combined_info_df = combined_info_df.drop_duplicates()
combined_info_df.to_csv('../data/plants_updated_certification_and_info.csv', index=False)

# %%
