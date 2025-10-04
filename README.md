**Global Cricket Match Data Analysis (2000 - Present)**

**Project Overview**

This repository hosts the Extract, Transform, and Load (ETL) pipeline, analytical SQL queries, and Exploratory Data Analysis (EDA) visualizations for a comprehensive analysis of global cricket match data sourced from Cricsheet. The project processes data across four major formats: Test, One Day Internationals (ODI), Twenty20 Internationals (T20I), and the Indian Premier League (IPL).

The primary goal is to transform raw JSON match data into a structured SQLite database, run complex analytical queries, and generate visual insights into historical trends, player performance, and match outcomes.

**Repository Structure and Key Files**

The project is structured to separate the data processing, analysis, and visualization logic.

File/Folder and Description

transform_load.py

ETL Pipeline. This script reads raw JSON files, transforms the data into structured tables (*_matches and *_deliveries), and loads them into the cricket_data.db SQLite database.

data_visuals.py

Visualization. This script connects to the generated database, executes complex SQL queries to pull summarized data, and uses Matplotlib/Seaborn to generate 10 key visualizations (V1.png - V10.png) into the data/results/visuals directory.

simplified_queries.sql

Analysis Layer. Contains 20 detailed SQL queries using UNION ALL to perform cross-format analysis (e.g., top batsmen across T20/IPL, toss impact across all formats).

.gitignore

Crucial for large files. This file ensures that all large raw data files, compressed zips (*.zip), the generated database (cricket_data.db), and resulting visualization images are excluded from the Git repository.

data/raw_json/

Placeholder directory for the original Cricsheet JSON files (ignored by Git).

data/raw_json_zips/

Placeholder directory for compressed raw data (ignored by Git).

**Getting Started**

Prerequisites
1.Python 3.x

2.Required Libraries:

pip install pandas sqlalchemy matplotlib seaborn


3.Data: Ensure you have the Cricsheet match data (JSON files) placed in the appropriate format-specific subdirectories within data/raw_json/.

Execution Steps
1.Initialize the Environment (assuming you have your data in the correct local structure):

# 1. Initialize Git and ensure correct branch name
git init
git branch -M main

# 2. Link to your SSH remote (replace with your personal link)
git remote add origin git@github.com:Divzdj/Cricsheet_Match_Data_Analysis.git

# 3. Commit and Overwrite (this ensures the repo is clean)
git add .
git commit -m "Final project file structure and .gitignore added."
git push --force origin main


2.Run the ETL Process (Database Creation):

python transform_load.py


This step creates the cricket_data.db file.

3.Run the Visualization Process (Generate Plots):

python data_visuals.py


This step generates  PNG image files in the data/results/visuals folder.

**Analytical Results and Interactive Dashboard**

The most significant findings from this analysis have been compiled into an interactive Tableau dashboard, allowing for detailed exploration of match trends, venue performance, and player statistics across all formats.

Click here to view the live interactive dashboard:

Global Cricket Performance Analysis (2000 - Present) - Tableau Dashboard
