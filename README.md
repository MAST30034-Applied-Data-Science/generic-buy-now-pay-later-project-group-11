# Generic Buy Now, Pay Later Project

**Members:** 
- Andrew 
- Jas Min Chen
- Nadya Aurelia Herryyanto
- Patrick Lourenz
- Roseline

## Introduction
Merchants are looking to boost their customer base by forming a partnership with this firm and in return, the BNPL firm gets a small percentage of revenue (take rate) to cover operating costs. However, the BNPL firm can only onboard at most 100 number of merchants every year due to limited resources. 

This model will define the top 100 best merchants for the BNPL platform by predicting its revenue contribution for the next 6 months. Additionally, decide what features and heuristics greatly separate merchants that should and should not be selected?

## Datasets
BNPL Platform Dataset:
- Daily Transaction (2021-02-28 to 2022-08-28)
- Consumer/Merchant Fraud Probability
- Consumer Details
- Merchant Details

External Dataset: 
- 2021 Age population by SA2, 
- 2016 Income by SA2, 
- 2016 Postcode boundaries,
- 2016 SA2 boundaries, 
- 2011 Postcode to SA2.

## Dependencies
- Python 3.9.7
- Required Libraries available in requirements.txt

## Guide
To run the pipeline, please visit the project directory and follow the following steps in order:
1. Get the credentials needed in order to access the AURIN API through this website: https://adp-access.aurin.org.au/data-provider
2. Create a `.env` file and write the following details in the file:

    `WFS_USERNAME={Insert Username Here}`

    `WFS_PASSWORD={Insert Password Here}`
3. Run `scripts\download.py`: This downloads the external datasets into the `data/abs` directory.

-- Alternatively --
For marking purpose, external datasets can be downloaded through this link: https://mega.nz/folder/bzx02ThK#lQxd0ZsDLEhd9Zbqj9D7gA. The 11 files should be saved into the `data/abs` directory. Create `data/abs` folder when folder is not present. (If this step is to be performed, skip step 1 - 3)

4. Run `notebook\poa_to_sa2.ipynb`: Creates lookup dataframe that can translate postcodes to SA2.
5. Run `scripts\ETL.py`: This consolidates and transforms BNPL and external datasets into one dataframe and saves into `data/curated` directory.
6. Run `notebook\outlier_detection_removal.ipynb`: Detects and removes outliers by product categories and updates the dataframe from ETL.py.
7. Run `scripts\fraud.py`: Remove fraudulent transaction data on decided threshold, and save output in `data/curated` directory.
8. Run `notebook\feature_engineering.ipynb`: Transforms new variables, utilizing data from external and internal datasets. Saves output files to `data/curated`.
9. Run `models\model.ipynb`: Rank top 10 merchants from each specified merchant segments and top 100 overall. Saves output files to `data/curated`.

To read the two analysis performed, please visit these directories:
- Fraud analysis `notebook\fraud_analysis.ipynb`.
- Tag and geospatial visualization analysis `notebook\tag_and_geospatial_analysis.ipynb`.

To read the summary of the whole project, please visit `notebook\summary_notebook.ipynb`.

## Directory
- `.github`: Contains Github Classroom Feedback (rendered at initialising Github repository).
- `data`: Contains `tables` where provided data files are stored and `curated` data files.
- `models`: Contains notebook to train and evaluate model.
- `notebooks`: Contains all the notebooks to run the pipeline.
- `plots`: Contains all exported visualizations (EDA & Model visualizations).
- `scripts`: Contains ETL script that extracts external datases.
