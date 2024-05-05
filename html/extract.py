# extract.py

import sys
import pandas as pd

def extract_data(file_path):
    # Example: Read data from a CSV file
    df = pd.read_csv(file_path)
    return df.head()  # Return the first few rows of the DataFrame

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python extract.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    extracted_data = extract_data(file_path)
    print(extracted_data)
