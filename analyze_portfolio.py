import pandas as pd
import warnings
from datetime import datetime
import os

# --- 1. CONFIGURATION ---
DATA_DIRECTORY = 'data'
RESULTS_DIRECTORY = 'results'

INPUT_PORTFOLIO_FILE = os.path.join(DATA_DIRECTORY, 'MyPortfolio.ods')
OUTPUT_PORTFOLIO_FILE = os.path.join(RESULTS_DIRECTORY, 'MyPortfolio_Analyzed.ods')
SUM_TOLERANCE = 0.001 # 0.1% margin of error for sums

def categorize_holding(holding, rules):
    """
    Categorizes a single holding based on a prioritized list of rules.
    This includes logic for bond duration.
    """
    symbol = str(holding.get('Symbol', ''))
    description = str(holding.get('Description', '')).upper()
    maturity_date = holding.get('Maturity Date')

    # --- Waterfall Logic ---
    # Priority 1: Exact Matches
    for _, rule in rules[rules['Rule Type'] == 'EXACT_MATCH'].iterrows():
        if symbol == rule['Parameter']:
            return rule['Master Category']

    # Priority 2: Strong Patterns (Starts With, Ends With)
    for _, rule in rules[rules['Rule Type'] == 'STARTS_WITH'].iterrows():
        if symbol.startswith(str(rule['Parameter'])):
            return rule['Master Category']
    
    for _, rule in rules[rules['Rule Type'] == 'ENDS_WITH'].iterrows():
        if symbol.endswith(str(rule['Parameter'])):
            return rule['Master Category']
            
    # Priority 3: Duration-based rules for bonds/CDs
    if pd.notna(maturity_date):
        try:
            maturity_dt = pd.to_datetime(maturity_date)
            years_to_maturity = (maturity_dt - datetime.now()).days / 365.25
            
            if years_to_maturity <= 2:
                return 'Bonds - Short (0-2y)'
            elif years_to_maturity <= 10:
                return 'Bonds - Interm (3-10y)'
            else:
                return 'Bonds - Long (10+y)'
        except:
            pass # Ignore malformed maturity dates
            
    # Priority 4: Description Keywords
    for _, rule in rules[rules['Rule Type'] == 'CONTAINS_DESC'].iterrows():
        if str(rule['Parameter']).upper() in description:
            return rule['Master Category']
            
    # Priority 5: General Rules (e.g., typical stock tickers)
    for _, rule in rules[rules['Rule Type'] == 'LEN_ALPHA'].iterrows():
        try:
            min_len, max_len = map(int, rule['Parameter'].split('-'))
            if min_len <= len(symbol) <= max_len and symbol.isalpha():
                return rule['Master Category']
        except:
            pass # Ignore malformed LEN_ALPHA rules
            
    # Priority 6: Fallback
    return 'Uncategorized'

def analyze_portfolio():
    """ Main function to run the complete portfolio analysis. """
    print("--- Starting Portfolio Analysis ---")
    try:
        # Create the results directory if it doesn't exist
        if not os.path.exists(RESULTS_DIRECTORY):
            print(f"Creating results directory: '{RESULTS_DIRECTORY}'")
            os.makedirs(RESULTS_DIRECTORY)
        
        # --- 2. LOAD AND VALIDATE CONFIGURATION ---
        print("Reading and validating configuration...")
        xls = pd.ExcelFile(INPUT_PORTFOLIO_FILE, engine='odf')
        config_targets_df = xls.parse('Config_Targets')
        config_ticker_map_df = xls.parse('Config_TickerMap')
        
        if 'Config_DCA' in xls.sheet_names:
            config_dca_df = xls.parse('Config_DCA')
            print("DCA configuration loaded.")
        else:
            config_dca_df = pd.DataFrame(columns=['Master Category', 'Time Horizon (Years)', 'Monthly Contribution'])
            print("INFO: 'Config_DCA' tab not found. Skipping projections.")
            
        config_targets_df = config_targets_df[config_targets_df['Master Category'] != 'Total'].copy()
        target_sum = config_targets_df['Target Percent'].sum()
        if abs(target_sum - 1.0) > SUM_TOLERANCE:
            print(f"\n!!! WARNING: Target percentages in 'Config_Targets' add up to {target_sum:.2%}, not 100%.")
        
        print("Configuration loaded successfully.")

        # --- 3. LOAD AND CONSOLIDATE DATA ---
        all_data_tabs = [sheet for sheet in xls.sheet_names if sheet.startswith('Data_')]
        if not all_data_tabs:
            print("\nERROR: No data tabs found. Make sure you have sheets named with the 'Data_' prefix.")
            return
        
        all_holdings_list = []
        for tab in all_data_tabs:
            df = xls.parse(tab)
            df['Source Tab'] = tab
            all_holdings_list.append(df)
        all_holdings_df = pd.concat(all_holdings_list, ignore_index=True)
        print(f"Consolidated {len(all_holdings_df)} total holdings.")

        # --- 4. CATEGORIZE AND VALIDATE HOLDINGS ---
        print("Categorizing all holdings...")
        all_holdings_df['Master Category'] = all_holdings_df.apply(
            lambda row: categorize_holding(row, config_ticker_map_df), axis=1
        )

        uncategorized_df = all_holdings_df[all_holdings_df['Master Category'] == 'Uncategorized']
        if not uncategorized_df.empty:
            print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("!!! WARNING: Uncategorized Holdings Found !!!")
            print("The following items could not be categorized. Please add rules for them in Config_TickerMap:")
            print(uncategorized_df[['Symbol', 'Description', 'Current Value']].to_string(index=False))
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
        else:
            print("All holdings were categorized successfully.")

        # --- 5. CALCULATE SUMMARIES AND RUN FINAL CHECKS ---
        print("Calculating portfolio summary...")
        all_holdings_df['Current Value'] = pd.to_numeric(all_holdings_df['Current Value'], errors='coerce').fillna(0)
        total_portfolio_value = all_holdings_df['Current Value'].sum()

        current_allocations = all_holdings_df.groupby('Master Category')['Current Value'].sum().reset_index()
        current_allocations.rename(columns={'Current Value': 'Current Amount'}, inplace=True)

        defined_categories = set(config_targets_df['Master Category'])
        found_categories = set(current_allocations['Master Category']) - {'Uncategorized'}
        missing_in_config = found_categories - defined_categories
        if missing_in_config:
            print("\n!!! CRITICAL ERROR: Category Mismatch Found !!!")
            print("The following categories were found in your data but are MISSING from your 'Config_Targets' tab:")
            for cat in sorted(list(missing_in_config)):
                print(f"  - {cat}")
            print("Please add these categories to 'Config_Targets' to ensure correct reporting.")
            return

        summary_df = pd.merge(config_targets_df, current_allocations, on='Master Category', how='left').fillna(0)
        
        if total_portfolio_value > 0:
            summary_df['Target Amount'] = summary_df['Target Percent'] * total_portfolio_value
            summary_df['Current Percent'] = summary_df['Current Amount'] / total_portfolio_value
        else:
            summary_df['Target Amount'], summary_df['Current Percent'] = 0, 0
        summary_df['Difference $'] = summary_df['Current Amount'] - summary_df['Target Amount']
        
        reporting_summary_df = summary_df.groupby('Reporting Category')[['Current Amount', 'Target Amount']].sum().reset_index()
        if total_portfolio_value > 0:
            reporting_summary_df['Current Percent'] = reporting_summary_df['Current Amount'] / total_portfolio_value
            reporting_summary_df['Target Percent'] = reporting_summary_df['Target Amount'] / total_portfolio_value
        else:
            reporting_summary_df['Current Percent'], reporting_summary_df['Target Percent'] = 0, 0
        
        print("Calculating DCA projections...")
        projections_df = pd.merge(summary_df, config_dca_df, on='Master Category', how='left').fillna(0)
        projections_df = projections_df[projections_df['Difference $'] < 0].copy()
        projections_df['Amount Under Target'] = -projections_df['Difference $']
        
        projections_df['Glide Path Monthly Target'] = projections_df.apply(
            lambda row: (row['Amount Under Target'] / (row['Time Horizon (Years)'] * 12)) if row['Time Horizon (Years)'] > 0 else 0,
            axis=1
        )
        projections_df['Monthly Shortfall/Surplus'] = projections_df['Monthly Contribution'] - projections_df['Glide Path Monthly Target']
        
        projections_output_df = projections_df[[
            'Master Category', 'Amount Under Target', 'Time Horizon (Years)',
            'Glide Path Monthly Target', 'Monthly Contribution', 'Monthly Shortfall/Surplus'
        ]].copy()
        print("Calculations complete.")

        # --- 6. WRITE ALL OUTPUTS TO A NEW SPREADSHEET ---
        print(f"Preparing to write all results to a new file: '{OUTPUT_PORTFOLIO_FILE}'")
        with pd.ExcelWriter(OUTPUT_PORTFOLIO_FILE, engine='odf') as writer:
            # Write back original config and data tabs
            config_targets_df.to_excel(writer, sheet_name='Config_Targets', index=False)
            config_ticker_map_df.to_excel(writer, sheet_name='Config_TickerMap', index=False)
            for df_original in all_holdings_list:
                tab_name = df_original['Source Tab'].iloc[0]
                df_original.drop(columns=['Source Tab']).to_excel(writer, sheet_name=tab_name, index=False)
            
            # Write verified data tabs
            for tab in all_data_tabs:
                verified_df = all_holdings_df[all_holdings_df['Source Tab'] == tab].drop(columns=['Source Tab'])
                verified_df.to_excel(writer, sheet_name=f"Verified_{tab}", index=False)
            
            # Format and write Master Summary
            money_cols_master = ['Current Amount', 'Target Amount', 'Difference $']
            for col in money_cols_master:
                summary_df[col] = summary_df[col].map('${:,.2f}'.format)
            percent_cols_master = ['Target Percent', 'Current Percent']
            for col in percent_cols_master:
                summary_df[col] = (summary_df[col] * 100).map('{:.1f}%'.format)
            summary_df.to_excel(writer, sheet_name='Output_MasterSummary', index=False)
            print("Wrote master summary tab.")

            # Format and write High-Level Summary
            money_cols_report = ['Current Amount', 'Target Amount']
            for col in money_cols_report:
                reporting_summary_df[col] = reporting_summary_df[col].map('${:,.2f}'.format)
            percent_cols_report = ['Target Percent', 'Current Percent']
            for col in percent_cols_report:
                reporting_summary_df[col] = (reporting_summary_df[col] * 100).map('{:.1f}%'.format)
            reporting_summary_df.to_excel(writer, sheet_name='Output_GrowthVsStable', index=False)
            print("Wrote high-level summary tab.")

            # Format and write DCA Projections
            if not projections_output_df.empty:
                money_cols_dca = ['Amount Under Target', 'Glide Path Monthly Target', 'Monthly Contribution', 'Monthly Shortfall/Surplus']
                for col in money_cols_dca:
                    projections_output_df[col] = projections_output_df[col].map('${:,.2f}'.format)
                projections_output_df.to_excel(writer, sheet_name='Output_Projections', index=False)
                print("Wrote DCA projections tab.")

        print(f"\nSUCCESS: A new file '{OUTPUT_PORTFOLIO_FILE}' has been created with the full analysis.")

    except FileNotFoundError:
        print(f"\nERROR: The input file '{INPUT_PORTFOLIO_FILE}' was not found in the '{DATA_DIRECTORY}' folder.")
    except PermissionError:
        print("\n--------------------------------------------------------------------")
        print(f"ERROR: Could not write to the output file.")
        print("The file is likely open in another application (like LibreOffice).")
        print("\nPlease close BOTH the input and output spreadsheets and run the script again.")
        print("--------------------------------------------------------------------")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == '__main__':
    warnings.simplefilter("ignore")
    analyze_portfolio()