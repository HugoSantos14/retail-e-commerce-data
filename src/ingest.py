import os
import shutil
from pathlib import Path
import pandas as pd
import kagglehub

KAGGLE_DATASET = 'mashlyn/online-retail-ii-uci'
SOURCE_FILE = 'online_retail_II.csv'

def download_kaggle_dataset():
    print('[INFO] Downloading dataset from Kaggle... this may take a moment.')
    try:
        download_path = kagglehub.dataset_download(KAGGLE_DATASET)
        print('[INFO] Download complete!')
        return Path(download_path)
    except Exception as e:
        print(f'[ERROR] Failed to download dataset from Kaggle: {e}')
        exit()

def main():
    ROOT_PATH = Path(__file__).parent.parent
    BRONZE_DIR = ROOT_PATH / 'data' / 'bronze'
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_FILE_PATH = BRONZE_DIR / 'online_retail_II.csv'
    
    print(f'[SETUP] Program root: {ROOT_PATH}')
    print(f'[SETUP] Bronze layer: {BRONZE_DIR}')
    
    if not BRONZE_FILE_PATH.exists():
        print(f'[INFO] File not found at {BRONZE_FILE_PATH}. Starting ingestion...')
        download_path = download_kaggle_dataset()
        source_file_path = download_path / SOURCE_FILE
        
        if source_file_path.exists():
            print(f'[INFO] Source file found. Copying to bronze layer...')
            shutil.copy(source_file_path, BRONZE_FILE_PATH)
            print(f'[INFO] Ingestion complete! File is now available at {BRONZE_FILE_PATH}.')
        else:
            print(f'[ERROR] Expected file {SOURCE_FILE} not found in download path: {download_path}')
            return
    else:
        print('[INFO] Dataset file already exists. Skipping ingestion.')
    
    df = pd.read_csv(BRONZE_FILE_PATH)
    print(df.info())
    
if __name__ == '__main__':
    main()