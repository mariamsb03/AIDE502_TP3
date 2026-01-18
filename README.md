# Data Pipeline with SQL and NoSQL Integration - Windows Edition

This will execute all stages in order:
1. Start Docker services (LocalStack, MySQL, MongoDB)
2. Create S3 buckets
3. Download WikiText-2 dataset
4. Upload data to raw bucket (build\unpack_to_raw.py)
5. Clean and load data to MySQL staging (src\preprocess_to_staging.py)
6. Tokenize and load data to MongoDB curated (src\process_to_curated.py)


## 🎯 Expected Output

After successful execution, you should see:
- ✅ 3 S3 buckets created
- ✅ WikiText data downloaded
- ✅ ~36,000+ texts in MySQL staging
- ✅ ~36,000+ tokenized documents in MongoDB curated
- ✅ All log files created

## Additional
There are some files as:

  - dataset.py this downloads the dataset
  - try_sqlite.py: this is to test the SQLite
  - tests folder: inside there are 2 python files that queries the data at each stage (staging & curated)
  - dvc.yaml: is the whole pipeline automated
