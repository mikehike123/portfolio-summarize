import pandas as pd
import warnings
from datetime import datetime
import os
import re

# --- 1. CONFIGURATION ---
DATA_DIRECTORY = 'data'
RESULTS_DIRECTORY = 'results'

INPUT_PORTFOLIO_FILE = os.path.join(DATA_DIRECTORY, 'MyPortfolio.ods')
OUTPUT_PORTFOLIO_FILE = os.path.join(RESULTS_DIRECTORY, 'MyPortfolio_Analyzed.ods')
SUM_TOLERANCE = 0.001

def categorize_holding(holding, rules):
    """
    Categorizes a single holding using a hybrid, prioritized waterfall logic.
    """
    symbol = str(holding.get('Symbol', ''))
    description = str(holding.get('Description', '')).upper()
    manual_maturity_date = holding.get('Maturity Date')

    # --- Waterfall Logic ---
    for _, rule in rules[rules['Rule Type'] == 'EXACT_MATCH'].iterrows():
        if symbol == rule['Parameter']:
            return rule['Master Category']

    for _, rule in rules[rules['Rule Type'] == 'ENDS_WITH'].iterrows():
        if symbol.endswith(str(rule['Parameter'])):
            return rule['Master Category']
    
    if pd.notna(manual_maturity_date):
        try:
            maturity_dt = pd.to_datetime(manual_maturity_date)
            years_to_maturity = (maturity_dt - datetime.now()).days / 365.25
            if years_to_maturity <= 2: return 'Bonds - Short (0-2y)'
            elif years_to_maturity <= 10: return 'Bonds - Interm (3-10y)'
            else: return 'Bonds - Long (10+y)'
        except (ValueError, TypeError): pass
    elif 'TREAS' in description or 'CD' in description:
        date_match = re.search(r'\d{1,2}/\d{1,2}/\d{4}', description)
        if date_match:
            try:
                maturity_dt = pd.to_datetime(date_match.group(0))
                years_to_maturity = (maturity_dt - datetime.now()).days / 365.25
                if years_to_maturity <= 2: return 'Bonds - Short (0-2y)'
                elif years_to_maturity <= 10: return 'Bonds - Interm (3-10y)'
                else: return 'Bonds - Long (10+y)'
            except (ValueError, TypeError): pass
            
    for _, rule in rules[rules['Rule Type'] == 'CONTAINS_DESC'].iterrows():
        if str(rule['Parameter']).upper() in description:
            return rule['Master Category']
            
    for _, rule in rules[rules['Rule Type'] == 'LEN_ALPHA'].iterrows():
        try:
            min_len, max_len = map(int, rule['Parameter'].split('-'))
            if min_len <= len(symbol) <= max_len and symbol.isalpha():
                return rule['Master Category']
        except: pass
            
    return 'Uncategorized'

def analyze_portfolio():
    """ Main function to run the complete portfolio analysis. """
    print("--- Starting Portfolio Analysis ---")
    try:
        if not os.path.exists(RESULTS_DIRECTORY):
            print(f"Creating results directory: '{RESULTS_DIRECTORY}'")
            os.makedirs(RESULTS_DIRECTORY)
        
        print("Reading and validating configuration...")
        xls = pd.ExcelFile(INPUT_PORTFOLIO_FILE, engine='odf')
        config_targets_df = xls.parse('Config_Targets')
        config_ticker_map_df = xls.parse('Config_TickerMap')
        
        dca_tab_exists = 'Config_DCA' in xls.sheet_names
        if dca_tab_exists:
            config_dca_df = xls.parse('Config_DCA')
            
            # --- NEW FIX: Clean the data read from the DCA config tab ---
            # This ensures that numbers formatted as text (e.g., '$7,124.94') are converted to actual numbers for calculation.
            config_dca_df['Monthly Contribution'] = pd.to_numeric(
                config_dca_df['Monthly Contribution'].astype(str).replace({'\$': '', ',': ''}, regex=True),
                errors='coerce'
            ).fillna(0)
            
            config_dca_df['Time Horizon (Years)'] = pd.to_numeric(
                config_dca_df['Time Horizon (Years)'],
                errors='coerce'
            ).fillna(0)
            # --- END FIX ---
            
            print("DCA configuration loaded. Running in MONITOR mode.")
        else:
            config_dca_df = pd.DataFrame()
            print("INFO: 'Config_DCA' tab not found. Running in DISCOVER mode.")
            
        config_targets_df = config_targets_df[config_targets_df['Master Category'].str.lower() != 'total'].copy()
        target_sum = config_targets_df['Target Percent'].sum()
        if abs(target_sum - 1.0) > SUM_TOLERANCE:
            print(f"\n!!! WARNING: Target percentages in 'Config_Targets' add up to {target_sum:.2%}, not 100%.")

        print("Configuration loaded successfully.")

        all_data_tabs = [sheet for sheet in xls.sheet_names if sheet.startswith('Data_')]
        all_holdings_list = []
        for tab in all_data_tabs:
            df = xls.parse(tab)
            df['Source Tab'] = tab
            all_holdings_list.append(df)
        all_holdings_df = pd.concat(all_holdings_list, ignore_index=True)
        print(f"Consolidated {len(all_holdings_df)} total holdings.")

        print("Categorizing all holdings...")
        all_holdings_df['Master Category'] = all_holdings_df.apply(lambda row: categorize_holding(row, config_ticker_map_df), axis=1)
        uncategorized_df = all_holdings_df[all_holdings_df['Master Category'] == 'Uncategorized']
        if not uncategorized_df.empty:
            print("\n!!! WARNING: Uncategorized Holdings Found !!!")
            print(uncategorized_df[['Symbol', 'Description', 'Current Value']].to_string(index=False))
        else:
            print("All holdings were categorized successfully.")

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
            for cat in sorted(list(missing_in_config)): print(f"  - '{cat}' was found in data but is MISSING from 'Config_Targets'.")
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
        
        underweight_df = summary_df[summary_df['Difference $'] < 0].copy()
        underweight_df['Amount Under Target'] = -underweight_df['Difference $']
        
        print("Calculations complete.")

        print(f"Writing all results to new file: '{OUTPUT_PORTFOLIO_FILE}'")
        with pd.ExcelWriter(OUTPUT_PORTFOLIO_FILE, engine='odf') as writer:
            # (The rest of the script is identical to the previous version)
            config_targets_df.to_excel(writer, sheet_name='Config_Targets', index=False)
            config_ticker_map_df.to_excel(writer, sheet_name='Config_TickerMap', index=False)
            if dca_tab_exists: config_dca_df.to_excel(writer, sheet_name='Config_DCA', index=False)
            for df_original in all_holdings_list:
                tab_name = df_original['Source Tab'].iloc[0]
                df_original.drop(columns=['Source Tab']).to_excel(writer, sheet_name=tab_name, index=False)
            for tab in all_data_tabs:
                verified_df = all_holdings_df[all_holdings_df['Source Tab'] == tab].drop(columns=['Source Tab'])
                verified_df.to_excel(writer, sheet_name=f"Verified_{tab}", index=False)
            
            summary_to_write = summary_df.copy()
            money_cols = ['Current Amount', 'Target Amount', 'Difference $']
            for col in money_cols: summary_to_write[col] = summary_to_write[col].map('${:,.2f}'.format)
            summary_to_write['Target Percent'] = (summary_to_write['Target Percent'] * 100).map('{:.1f}%'.format)
            summary_to_write['Current Percent'] = (summary_to_write['Current Percent'] * 100).map('{:.1f}%'.format)
            summary_to_write.to_excel(writer, sheet_name='Output_MasterSummary', index=False)
            print("Wrote master summary tab.")

            report_to_write = reporting_summary_df.copy()
            money_cols_report = ['Current Amount', 'Target Amount']
            for col in money_cols_report: report_to_write[col] = report_to_write[col].map('${:,.2f}'.format)
            report_to_write['Target Percent'] = (report_to_write['Target Percent'] * 100).map('{:.1f}%'.format)
            report_to_write['Current Percent'] = (report_to_write['Current Percent'] * 100).map('{:.1f}%'.format)
            report_to_write.to_excel(writer, sheet_name='Output_GrowthVsStable', index=False)
            print("Wrote high-level summary tab.")

            if not dca_tab_exists:
                if not underweight_df.empty:
                    print("Generating DCA planning tabs...")
                    dca_options_df = underweight_df[['Master Category', 'Amount Under Target']].copy()
                    for years in [1, 2, 3, 4, 5]:
                        dca_options_df[f'{years}-Year Monthly'] = dca_options_df['Amount Under Target'] / (years * 12)
                    money_cols_options = [col for col in dca_options_df.columns if 'Monthly' in col or 'Amount' in col]
                    for col in money_cols_options: dca_options_df[col] = dca_options_df[col].map('${:,.2f}'.format)
                    dca_options_df.to_excel(writer, sheet_name='Output_DCA_Options', index=False)
                    print("Wrote DCA options tab.")

                    dca_template = pd.DataFrame()
                    dca_template['Master Category'] = underweight_df['Master Category']
                    dca_template['Amount Under Target'] = underweight_df['Amount Under Target']
                    dca_template['Time Horizon (Years)'] = ''
                    dca_template['Monthly Contribution'] = ''
                    cash_available = current_allocations.loc[current_allocations['Master Category'] == 'Cash', 'Current Amount'].sum()
                    required_1_year = underweight_df['Amount Under Target'].sum() / 12
                    summary_rows = [
                        { 'Master Category': '_FROM_NEW_CASH_', 'Amount Under Target': '', 'Time Horizon (Years)': '', 'Monthly Contribution': '' },
                        { 'Master Category': '', 'Amount Under Target': '', 'Time Horizon (Years)': '', 'Monthly Contribution': '' },
                        { 'Master Category': 'Available Cash (Pre-DCA)', 'Amount Under Target': cash_available, 'Time Horizon (Years)': '', 'Monthly Contribution': '' },
                        { 'Master Category': 'Total Required (if 1-Year Plan)', 'Amount Under Target': required_1_year, 'Time Horizon (Years)': '', 'Monthly Contribution': '' },
                        { 'Master Category': 'Cash Remaining (if 1-Year Plan)', 'Amount Under Target': cash_available - required_1_year, 'Time Horizon (Years)': '', 'Monthly Contribution': '' }
                    ]
                    dca_template = pd.concat([dca_template, pd.DataFrame(summary_rows)], ignore_index=True)
                    dca_template['Amount Under Target'] = dca_template['Amount Under Target'].map(lambda x: '${:,.2f}'.format(x) if isinstance(x, (int, float)) else x)
                    dca_template.to_excel(writer, sheet_name='Output_DCA_Template', index=False)
                    print("Wrote DCA template tab.")
            
            else: # Monitor Mode
                print("Generating DCA projections and rebalancing plan...")
                projections_df = pd.merge(summary_df.copy(), config_dca_df, on='Master Category', how='left').fillna(0)
                underweight_df = projections_df[projections_df['Difference $'] < 0].copy()
                
                if not underweight_df.empty:
                    underweight_df['Amount Under Target'] = -underweight_df['Difference $']
                    underweight_df['Required Monthly Investment'] = underweight_df.apply(lambda r: (r['Amount Under Target'] / (r['Time Horizon (Years)'] * 12)) if r['Time Horizon (Years)'] > 0 else 0, axis=1)
                    underweight_df['Projected Total Contribution'] = underweight_df['Monthly Contribution'] * underweight_df['Time Horizon (Years)'] * 12
                    underweight_df['Monthly Shortfall/Surplus'] = underweight_df['Monthly Contribution'] - underweight_df['Required Monthly Investment']
                    projections_output_df = underweight_df[['Master Category', 'Amount Under Target', 'Projected Total Contribution', 'Time Horizon (Years)', 'Required Monthly Investment', 'Monthly Contribution', 'Monthly Shortfall/Surplus']].copy()
                    
                    money_cols_dca = ['Amount Under Target', 'Projected Total Contribution', 'Required Monthly Investment', 'Monthly Contribution', 'Monthly Shortfall/Surplus']
                    for col in money_cols_dca: projections_output_df[col] = projections_output_df[col].map('${:,.2f}'.format)
                    projections_output_df.to_excel(writer, sheet_name='Output_Projections', index=False)
                    print("Wrote DCA projections tab.")

                new_cash_contribution = config_dca_df[config_dca_df['Master Category'] == '_FROM_NEW_CASH_']['Monthly Contribution'].sum()
                total_required_investment = underweight_df['Required Monthly Investment'].sum()
                funding_shortfall = total_required_investment - new_cash_contribution
                
                if funding_shortfall > 0:
                    overweight_df = summary_df[summary_df['Difference $'] > 0].copy()
                    rebalancing_plan_data = []
                    for _, row in underweight_df.iterrows():
                        rebalancing_plan_data.append({'Action': 'INVEST', 'Category': row['Master Category'], 'Monthly Plan': row['Required Monthly Investment']})
                    rebalancing_plan_data.append({'Action': '---', 'Category': '---', 'Monthly Plan': '---'})
                    rebalancing_plan_data.append({'Action': 'SOURCE', 'Category': 'From New Cash', 'Monthly Plan': new_cash_contribution})
                    for _, row in overweight_df.sort_values('Difference $', ascending=False).iterrows():
                        rebalancing_plan_data.append({'Action': 'SOURCE', 'Category': f"From {row['Master Category']} (Over by ${row['Difference $']:,.0f})", 'Monthly Plan': None})
                    rebalancing_plan_data.append({'Action': 'SHORTFALL', 'Category': 'Amount to source from overweight assets', 'Monthly Plan': funding_shortfall})
                    rebalancing_plan_df = pd.DataFrame(rebalancing_plan_data)
                    
                    rebalancing_plan_df['Monthly Plan'] = rebalancing_plan_df['Monthly Plan'].map(lambda x: '${:,.2f}'.format(x) if pd.notna(x) and isinstance(x, (int, float)) else x)
                    rebalancing_plan_df.to_excel(writer, sheet_name='Output_RebalancingPlan', index=False)
                    print("Wrote rebalancing plan tab.")

        print(f"\nSUCCESS: A new file '{OUTPUT_PORTFOLIO_FILE}' has been created.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == '__main__':
    warnings.simplefilter("ignore")
    analyze_portfolio()