
echo 'PARSING SOURCES'
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute yahoo_prices_raw_parsed_clean.ipynb 
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute yahoo_comp_details_raw_parsed_cleaned.ipynb 
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute nasdaq_news_raw_parsed_cleaned.ipynb 
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute nasdaq_comp_details_raw_parsed_cleaned.ipynb
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute bi_news_raw_parsed_clean.ipynb 
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute bi_comp_details_raw_parsed_cleaned.ipynb
echo 'PROCESSED DATA'
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute company_details_processed.ipynb
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute news_processed.ipynb 
echo 'ONTOLOGY DATA'
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute synergize_ontology.ipynb
echo 'DDL DATA'
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute company_details_ddf.ipynb 
jupyter nbconvert --ExecutePreprocessor.timeout=180 --to notebook --inplace --execute news_price_ddl.ipynb 
echo 'TRANSFORMING FILES DATA'
jupyter nbconvert  --ExecutePreprocessor.timeout=180  --to notebook --inplace --execute _spark_file_transforms.ipynb  
echo 'IMPORT TO NEO4j'
jupyter nbconvert --ExecutePreprocessor.timeout=600 --to notebook --inplace --execute neo4j_import.ipynb 
echo 'END'
rm data/_file_transforms/file_transforms.csv