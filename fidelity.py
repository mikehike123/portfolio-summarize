import pandas as pd
import gspread
import warnings # Keep the import

# --- Configuration ---
google_sheet_name = 'fidelity_accounts'
source_tab_name = 'Accounts Data'
output_tab_name = 'Cash Summary'
credentials_filename = 'credentials.json'

# --- NEW: Use a more direct filter to suppress the DeprecationWarning ---
# This is a broader filter but should be more effective.
warnings.simplefilter("ignore", DeprecationWarning)
# --- END NEW ---

# --- Main Script Logic ---
try:
    # --- Step 1: Authenticate and open the sheet ---
    print("Attempting to authenticate...")
    gc = gspread.service_account(filename=credentials_filename)
    spreadsheet = gc.open(google_sheet_name)
    print("Authentication successful. Spreadsheet opened.")

    # --- Step 2: Read data from the source tab ---
    worksheet = spreadsheet.worksheet(source_tab_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    print(f"Successfully read {len(df)} rows from '{source_tab_name}'.")

    # --- Step 3: Data Processing ---
    required_cols = ['Symbol', 'Current Value', 'Account Number']
    if not all(col in df.columns for col in required_cols):
        raise KeyError(f"One or more required columns not found in the sheet: {required_cols}")

    cash_accounts_df = df[df['Symbol'].astype(str).str.endswith('**', na=False)].copy()
    output_df = cash_accounts_df[['Account Number', 'Current Value']].rename(columns={
        'Account Number': 'Account',
        'Current Value': 'Cash'
    })
    print(f"Found {len(output_df)} cash accounts. Preparing to write to sheet...")

    # --- Step 4: Write the output DataFrame back to the sheet ---
    try:
        output_worksheet = spreadsheet.worksheet(output_tab_name)
        print(f"Found existing tab '{output_tab_name}'. Clearing it.")
        output_worksheet.clear()
    except gspread.WorksheetNotFound:
        print(f"Creating new tab '{output_tab_name}'.")
        output_worksheet = spreadsheet.add_worksheet(title=output_tab_name, rows="100", cols="20")

    # The update method, which is working correctly.
    output_worksheet.update(
        values=[output_df.columns.values.tolist()] + output_df.values.tolist(),
        range_name='A1'
    )

    print(f"Successfully wrote data to the '{output_tab_name}' tab.")

# --- Error Handling ---
# ... (Error handling remains the same) ...
except Exception as e:
    print(f"An unexpected error occurred: {e}")