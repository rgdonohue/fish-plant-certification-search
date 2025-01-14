# Environmental Certification Mining for Processing Plants

## Overview
This repository aims to automate the process of uncovering which fish processing plants hold specific environmental certifications. By leveraging Google’s Custom Search Engine (CSE) JSON API, we will identify certifications and link their sources for each plant.

## Objectives
- **Primary Goal:** Match each plant to its relevant environmental certification(s) and store the URL of the certification mention.
- **Secondary Goals:**
  - Identify plants claiming certification but not appearing in official databases.
  - Address missing entries in certification databases.

## Project Scope & Key Details

### Plant List
- **Total Plants:** ~1,200
  - **Without website links:** ~500
  - **With website links:** ~700

### Certifications
- **Certifying Bodies:**
  - ASC (Aquaculture Stewardship Council)
  - BAP (Best Aquaculture Practices)
  - Friend of the Sea (WSO)
  - FIP (Fisheries Improvement Project)
  - Marin Trust

### Search Terms
Each certification includes various keywords to ensure comprehensive results:
- **ASC:** `ASC | A.S.C. | “Aquaculture stewardship council” | aquaculture-stewardship-council | “A S C”`
- **BAP:** `BAP | B.A.P. | “Best aquaculture practices” | “Global seafood alliance” | GSA | G.S.A. | best-aquaculture-practices | “B A P”`
- **Friend of the Sea:** `"Friend of the sea" | FOS | F.O.S. | “World sustainability organization” | WSO | W.S.O. | friend-of-the-sea | “F O S”`
- **FIP:** `FIP | F.I.P. | “Fisheries improvement project” | fisheries-improvement-project | “F I P”`
- **Marin Trust:** `"Marin Trust" | marintrust | Marin-trust`

## Requirements

### Automated Search
1. **Google CSE JSON API:**
   - Automate keyword searches for each plant.
   - Parse JSON responses for relevant certification mentions.
2. **Site-Specific vs. General Search:**
   - **With website:** Perform site-specific searches (e.g., `site:example.com "ASC"`), including PDFs.
   - **Without website:** Perform broader searches using company name and keywords.

### Data Output
- **Spreadsheet Updates:**
  - Add matching certification URLs for each plant in the relevant columns.
  - Prioritize the highest-ranked result if multiple matches exist.

### Handling JSON Responses
- Parse results to extract and validate relevant URLs.
- Handle cases with no results or ensure correct results appear at the top.

## Deliverables

1. **Automated Script or Tool:**
   - Process ~1,200 plants.
   - Execute searches using certification keywords.
   - Populate the master spreadsheet with certification URLs.

2. **Documentation:**
   - Include all relevant code files.
   - Exclude sensitive information (API keys, passwords) for public sharing.

## Methodology

This project employed a two-phase approach to gather comprehensive certification data for fish processing plants. First, we analyzed plants with known websites through deep crawling to find certification mentions. Then, for plants lacking website information, we utilized Google's APIs to locate their websites and address details. This dual approach allowed us to maximize data collection while maintaining accuracy. The process involved website crawling, data enrichment through API calls, and final consolidation of all findings into a single, comprehensive dataset.

### 1. Initial Website Crawling
- Using `scripts/plants_websites_crawl.py`, we first analyzed plants with known websites
- The script performed a deep crawl of each website:
  - Searched up to 3 links deep from the homepage
  - Included PDF document analysis
  - Looked for specific certification keywords
  - Stored URLs where certification mentions were found
- Results were saved to `data/plants_with_info_certs_updated.csv`

### 2. Finding Missing Information
- For plants without websites, we used `scripts/find_missing_websites.py`
- The script leveraged two Google APIs:
  - Custom Search API: Located company websites
  - Places API: Retrieved address information
- For each company, we collected:
  - Official website URL
  - Country location
  - City
  - Site address
- Results were saved to `data/plants_without_info_updated.csv`

### 3. Secondary Certification Search
- The enhanced website crawler processed newly found websites
- Searched for certification mentions on these additional sites
- Updated certification columns with reference URLs
- Used the same deep crawling approach as the initial search

### 4. Data Consolidation
- `scripts/combine_data.py` merged all collected information:
  - Original plant data with certification references
  - Newly found address information
  - Additional certification mentions from new websites
- Produced final deliverable: `data/plants_updated_certification_and_info.csv`

## Usage

### Prerequisites
- Google CSE API Key
- Access to the master spreadsheet

### Steps
1. Configure API credentials in the script.
2. Run the script to process the plants.
3. Review the updated spreadsheet for validation.

## Future Enhancements
- Incorporate additional certifications.
- Improve keyword search robustness.
- Explore machine learning for enhanced result parsing.

## License
This project is licensed under the GNU General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.

---
For questions or support, please open an issue or contact the maintainers.

## Repository Structure

### Data Directory (`/data`)
- **Input:**
  - `plants_to_scrape_for_certs_original_01-03.csv`: Original plant data from Google Sheets
- **Output:**
  - `plants_with_info_certs_updated.csv`: Results from initial website crawling
  - `plants_without_info_updated.csv`: Results from Google API searches
  - `plants_updated_certification_and_info.csv`: Final consolidated dataset

### Python Scripts (`/scripts`)
- `plants_websites_crawl.py`: Crawls websites to find certification mentions
- `find_missing_websites.py`: Uses Google APIs to find missing website/address info
- `combine_data.py`: Merges all collected data into final dataset

### Jupyter Notebooks (`/notebooks`)
- Interactive versions of the scripts for development and testing
- Includes visualizations and data exploration
- Useful for troubleshooting and understanding the process

## Setup and Installation

### Environment Setup
1. Clone this repository:
   ```bash
   git clone [repository-url]
   cd [repository-name]
   ```

2. Create and activate a conda environment:
   ```bash
   conda create -n cert-mining python=3.8
   conda activate cert-mining
   ```

3. Install required packages:
   ```bash
   conda install --file conda_packages.txt
   ```

### API Configuration
1. Create a `.env` file in the root directory
2. Add your Google API credentials:
   ```
   GOOGLE_API_KEY=your_api_key_here
   GOOGLE_CSE_ID=your_custom_search_engine_id
   ```
   **Note:** Never commit the `.env` file to version control

### Running the Scripts
1. Ensure your `.env` file is properly configured
2. Run the scripts in order:
   ```bash
   python scripts/find_missing_websites.py
   python scripts/plants_websites_crawl.py
   python scripts/combine_data.py
   ```
