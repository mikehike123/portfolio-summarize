# Test publishing in git.
import pandas as pd

def format_fidelity_download(input_file='Portfolio_Positions_Aug-25-2025.csv', output_file='fidelity_cleaned.csv'):
    """
    Reads a raw CSV download from Fidelity, cleans it, selects essential columns,
    and saves it to a new, clean CSV file.
    """
    print(f"--- Running Fidelity Formatter ---")
    
    # Define the columns we want to keep from the original file.
    # The names must EXACTLY match the column headers in the Fidelity CSV.
    required_columns = [
        'Account Number',
        'Symbol',
        'Description',
        'Current Value'
    ]

    try:
        # Step 1: Read the input CSV file.
        print(f"Reading raw data from '{input_file}'...")
        df = pd.read_csv(input_file)

        # Step 2: Check if all required columns exist.
        for col in required_columns:
            if col not in df.columns:
                print(f"\nERROR: A required column '{col}' was not found in the input file.")
                print(f"Please make sure the downloaded CSV has the correct headers.")
                return # Stop the script

        # Step 3: Select only the columns we need.
        df_cleaned = df[required_columns].copy() # .copy() prevents a common pandas warning
        print("Selected the required columns.")

        # Step 4: Clean the 'Current Value' column.
        # This removes '$', ',', and converts the column to a numeric type.
        # errors='coerce' will turn any problematic values into empty cells (NaN).
        df_cleaned['Current Value'] = df_cleaned['Current Value'].replace({'\$': '', ',': ''}, regex=True)
        df_cleaned['Current Value'] = pd.to_numeric(df_cleaned['Current Value'], errors='coerce')

        # Step 5: Remove any rows where 'Current Value' is empty or zero, as these
        # are often summary rows or placeholders we don't need.
        df_cleaned.dropna(subset=['Current Value'], inplace=True) # Remove empty cells
        df_cleaned = df_cleaned[df_cleaned['Current Value'] != 0]
        print("Cleaned and formatted the data.")

        # Step 6: Write the clean DataFrame to the output file.
        # index=False prevents pandas from writing the row numbers to the file.
        df_cleaned.to_csv(output_file, index=False)
        print(f"\nSUCCESS: A clean file has been created at '{output_file}'")
        print(f"You can now copy the data from this file into your main portfolio workbook.")

    except FileNotFoundError:
        print(f"\nERROR: The input file '{input_file}' was not found.")
        print("Please make sure you have downloaded the Fidelity data and saved it in the correct folder.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

# This part allows the script to be run directly from the terminal.
if __name__ == '__main__':
    format_fidelity_download("Portfolio_fidelity_cindy.csv","fidelity_cindy_cleaned.csv")