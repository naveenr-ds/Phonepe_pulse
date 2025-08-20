# file: insurance_data_processor.py

import os
import json
import pandas as pd

def extract_insurance_data(base_path):

    data_list = []
    
    for state_dir in os.listdir(base_path):
        state_path = os.path.join(base_path, state_dir)
        if not os.path.isdir(state_path):
            continue

        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if not os.path.isdir(year_path):
                continue

            for file_name in os.listdir(year_path):
                if file_name.endswith(".json"):
                    file_path = os.path.join(year_path, file_name)
                    
                    with open(file_path, 'r') as f:
                        try:
                            content = json.load(f)
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in {file_path}: {e}")
                            continue

                    # Extract transaction data
                    transaction_data = content.get("data", {}).get("transactionData", [])
                    for item in transaction_data:
                        payment = item['paymentInstruments'][0]
                        data_list.append({
                            'states': state_dir,
                            'years': year_dir,
                            'quarter': int(file_name.strip('.json')),
                            'transaction_type': item['name'],
                            'transaction_count': payment['count'],
                            'transaction_amount': payment['amount']
                        })
    return data_list


def clean_state_names(df):
    """
    Clean and standardize state names in the DataFrame.
    """
    df = df.copy()  # Avoid modifying the original
    df['states'] = df['states'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar', regex=False)
    df['states'] = df['states'].str.replace('-', ' ')
    df['states'] = df['states'].str.title()
    df['states'] = df['states'].str.replace(
        'Dadra & Nagar Haveli & Daman & Diu',
        'Dadra and Nagar Haveli and Daman and Diu',
        regex=False
    )
    return df


def save_to_csv(df, output_path):
    """
    Save DataFrame to CSV.
    """
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")


def main():
    """
    Main execution function.
    """
    agg_ins_path = r"D:\DS-Projects\Phonepe\Final\pulse\data\aggregated\insurance\country\india\state"
    
    # Step 1: Extract data
    raw_data = extract_insurance_data(agg_ins_path)
    
    # Step 2: Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    # Step 3: Clean state names
    cleaned_df = clean_state_names(df)
    
    # Step 4: Save to CSV
    save_to_csv(cleaned_df, 'table1.csv')


if __name__ == "__main__":
    main()
