# Generic Buy Now, Pay Later Project

**Members:** 
- Andrew 
- Jas Min Chen
- Nadya Aurelia Herryyanto
- Patrick Lourenz
- Roseline

## Introduction
Project description here.

## Datasets
Datasets used here.

## Dependencies
- **Language:**
- **Packages/Libraries:**

## Guide
To run the pipeline, please visit the project directory and run the files in order:
1. `scripts\download.py`: This downloads the external datasets into the `data/abs` directory.
2. `notebook\poa_to_sa2.ipynb`: Creates lookup dataframe that can translate postcodes to SA2.
3. `scripts\ETL.py`: This consolidates and transforms BNPL and external datasets into one dataframe and saves into `data/curated` directory.
4. `notebook\outlier_detection_removal.ipynb`: Detects and removes outliers by product categories and updates the dataframe from ETL.py.
5. `notebook\feature_engineering.ipynb`: Transforms new variables, utilizing data from external and internal datasets. Saves output files to `data/curated`.
6. `notebook\model.ipynb`: Rank top 10 merchants from each specified merchant segments and Top 100 overall. Saves output files to `data/curated`.

## Directory
- `.github`: Contains Github Classroom Feedback (rendered at initialising Github repository).
- `data`: Contains `tables` where provided data files are stored and `curated` data files.
- `models`: Empty.
- `notebooks`: Contains all the notebooks to run the pipeline.
- `plots`: Contains all exported visualizations (EDA & Model visualizations).
- `scripts`: Contains ETL script that extracts external datases.
