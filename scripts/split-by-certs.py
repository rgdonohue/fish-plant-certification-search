# %%
import pandas as pd
# %%
plants_df = pd.read_csv('../data/plants_updated_certification_and_info.csv')
# %%
plants_df.columns
# %%
cert_columns = ['BAP Cert', 'ASC Cert', 'FOS Cert', 'FIP Cert', 'MarinTrust Cert']
new_rows = []

for _, row in plants_df.iterrows():
    for cert in cert_columns:
        urls = row.get(cert, '')
        if pd.notnull(urls):
            split_urls = [url.strip() for url in urls.split(';') if url.strip()]
            for url in split_urls:
                new_row = row.copy()
                new_row[cert] = url
                # Clear other certification columns
                for other_cert in cert_columns:
                    if other_cert != cert:
                        new_row[other_cert] = ''
                new_rows.append(new_row)

# Create a new DataFrame with the expanded rows
split_plants_df = pd.DataFrame(new_rows)

# Reset the index of the new DataFrame
split_plants_df.reset_index(drop=True, inplace=True)

# Replace the original DataFrame with the split DataFrame
plants_df = split_plants_df

# Save the updated DataFrame to a new CSV file
plants_df.to_csv('../data/plants_updated_certification_and_info_split.csv', index=False)

# %%
