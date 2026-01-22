-- Export table to processed data folder
COPY analysis_model
TO 'data/processed/analysis_model_2024.csv'
WITH (HEADER, DELIMITER ',');