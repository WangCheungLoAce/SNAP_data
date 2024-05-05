import zipfile
import pandas as pd

def extract_and_combine(zip_file_path):
    """Extract CSV data from a ZIP file and combine into a single DataFrame."""
    dfs = []

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            for filename in zf.namelist():
                if filename.lower().endswith('.csv'):
                    with zf.open(filename) as f:
                        df = pd.read_csv(f)
                        dfs.append(df)

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
        else:
            combined_df = pd.DataFrame()

        return combined_df

    except zipfile.BadZipFile:
        print("Error: Not a valid ZIP file.")
        return pd.DataFrame()

    except Exception as e:
        print(f"Error: {str(e)}")
        return pd.DataFrame()
