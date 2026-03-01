import pandas as pd
import os
import pip
from sklearn.model_selection import train_test_split
import logging


#ensure the "logs" directory exists
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

#logging config:
logger = logging.getLogger('data ingestion')
logger.setLevel(logging.DEBUG)

#setup the console handler:
console_handler = logging.StreamHandler() #the logs will print on the terminal.
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

#setup the log file path:
log_file_path = os.path.join(log_dir, 'data_ingestion.log') #this will create a file to store the logs, and for that we are giving the location of the file.
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

#setting up the formatter:
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

##defining the formatter for both handlers:
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

#loading the dataset:
def load_dataset(data_url: str) -> pd.DataFrame:
    """Loads the dataset from the specified URL and returns it as a pandas DataFrame."""
    try:
        logger.debug(f"Attempting to load dataset from {data_url}")
        df = pd.read_csv(data_url)
        logger.info("Dataset loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading dataset: %s", e)
        raise

#preprocess the data now:

def preprocess_data(data_url: str) -> pd.DataFrame:
    """pre processing the data by splitting it into training and testing sets."""
    try:
        df = load_dataset(data_url)
        df.drop(columns = ['Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4'], inplace = True)
        df.rename(columns = {df.columns[0]: 'target', df.columns[1]: 'text'}, inplace = True)
        logger.debug("Dataset preprocessing completed successfully.")
        return df
    except KeyError as e:
        logger.error(f"Error during data preprocessing: %s", e)
        raise
    except Exception as e:
        logger.error("An unexpected error occurred during data preprocessing: %s", e)
        raise

#save the data now:

import os
import pandas as pd
import logging

logger = logging.getLogger("data_ingestion")

def save_data(train_data: pd.DataFrame, test_data: pd.DataFrame, data_path: str) -> None:
    """Saves the training and testing data to data/raw directory."""
    try:
        # Create full raw directory path
        raw_data_path = os.path.join(data_path, "raw")

        # ✅ Create directory if it doesn't exist
        os.makedirs(raw_data_path, exist_ok=True)

        # File paths
        train_file_path = os.path.join(raw_data_path, "train.csv")
        test_file_path = os.path.join(raw_data_path, "test.csv")

        # Save files
        train_data.to_csv(train_file_path, index=False)
        test_data.to_csv(test_file_path, index=False)

        logger.info("Data saved successfully at %s", raw_data_path)

    except Exception as e:
        logger.error("Error saving data: %s", e)
        raise

#main function to execute the data ingestion process:
def main():
    try:
        test_size = 0.2
        # test_size = 0.2
        data_path = 'https://raw.githubusercontent.com/vikashishere/Datasets/main/spam.csv'
        df = load_dataset(data_url=data_path)
        final_df = preprocess_data(data_url=data_path)
        train_data, test_data = train_test_split(final_df, test_size=test_size, random_state=2)
        save_data(train_data, test_data, data_path='data')
    except Exception as e:
        logger.error('Failed to complete the data ingestion process: %s', e)
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
