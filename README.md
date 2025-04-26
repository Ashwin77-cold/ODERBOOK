import pandas as pd
import re
import os

# Load the Excel file
file_path = r'X:\DATA WORK\#Daily_Reportings\EXPIRY_OB\expiry ob\Updated_Orderbook.xlsx'

try:
    df = pd.read_excel(file_path)
except FileNotFoundError:
    print(f"File not found: {file_path}")
    exit()
except Exception as e:
    print(f"Error loading file: {e}")
    exit()

def extract_trading_symbol_details(trading_symbol):
    try:
        # Remove any trailing spaces
        trading_symbol = trading_symbol.strip()
        
        # Print original trading symbol for debugging
        print(f"Original Trading Symbol: {trading_symbol}")

        # Extract the option type (last two characters)
        option_type = trading_symbol[-2:]

        # Extract the last 7 characters before the option type to find the strike price
        strike_part = trading_symbol[-7:-2]  # Get the 5 digits for the strike price

        # Convert the strike price to an integer
        strike_price = int(strike_part)

        # Get the remaining part of the symbol by removing the last 7 characters
        symbol = trading_symbol[:-7].strip()

        # Keep only the alphabetic part of the symbol (removing any digits)
        final_symbol = re.sub(r'\d.*', '', symbol).strip()

        print(f"Extracted Symbol: {final_symbol}, Strike Price: {strike_price}, Option Type: {option_type}")  # Debugging line
        return final_symbol, strike_price, option_type
    except ValueError as ve:
        print(f"Value error extracting details from trading symbol '{trading_symbol}': {ve}")
        return None, None, None
    except Exception as e:
        print(f"Error extracting details from trading symbol '{trading_symbol}': {e}")
        return None, None, None

# Fill missing SYMBOL, STRIKE, and OPTION TYPE columns
for index, row in df.iterrows():
    if pd.isna(row['SYMBOL']):
        symbol, strike, option_type = extract_trading_symbol_details(row['TRADING SYMBOL'])
        if symbol:
            df.at[index, 'SYMBOL'] = symbol
            df.at[index, 'STRIKE'] = strike
            df.at[index, 'OPTION TYPE'] = option_type

# Dragging last filled row values for specified columns
columns_to_drag = ['AppOrderID', 'GeneratedBy', 'ExchangeInstrumentID', 'ExchangeSegment',
                   'OrderType', 'OrderPrice', 'OrderStopPrice', 'OrderStatus']

last_filled_row = df.dropna(subset=columns_to_drag).iloc[-1]

for col in columns_to_drag:
    df[col].fillna(last_filled_row[col], inplace=True)

# Setting ProductType to 'MIS'
df['ProductType'].fillna('MIS', inplace=True)

# Fill missing date values with the last non-missing value
df['OrderGeneratedDateTime'].fillna(method='ffill', inplace=True)
df['ExchangeTransactTime'].fillna(method='ffill', inplace=True)

# Function to update time to 15:30:00 only if not already set
def update_time_if_needed(datetime_str):
    if pd.isna(datetime_str) or not isinstance(datetime_str, str):
        return datetime_str
    # Split the date and time
    date_time_parts = datetime_str.split()
    if len(date_time_parts) == 2:
        date, time = date_time_parts
        if time == '15:30:00':
            return datetime_str
    elif len(date_time_parts) == 1:
        date = date_time_parts[0]
        return f"{date} 15:30:00"
    return datetime_str

df['OrderGeneratedDateTime'] = df['OrderGeneratedDateTime'].apply(update_time_if_needed)
df['ExchangeTransactTime'] = df['ExchangeTransactTime'].apply(update_time_if_needed)

# **New Task**: Read spot prices from a CSV based on the input date
input_date = input("Enter the date in YYYYMMDD format for SPOT: ")

# Construct the file name with the format SPOT_inputdate.csv
spot_file_path = f"Z:\\API_data_backup\\data\\RTD\\SPOT_{input_date}.csv"

try:
    spot_df = pd.read_csv(spot_file_path)
    spot_df.columns = spot_df.columns.str.strip()  # Strip extra spaces
    print(f"Loaded spot data from {spot_file_path}")

    # **New Addition**: Get and print the last modified time of the spot file
    last_modified_time = os.path.getmtime(spot_file_path)
    print(f"Last Updated Time of the Spot File: {pd.to_datetime(last_modified_time, unit='s')}")

except FileNotFoundError:
    print(f"Spot price file not found: {spot_file_path}")
    exit()
except Exception as e:
    print(f"Error loading spot price file: {e}")
    exit()

# **New Modification 1**: Print the spot names and prices
print("\nSpot Prices Used:")
for index, row in spot_df.iterrows():
    print(f"Name: {row['Name']}, Price: {row['Price']}")

# Create a dictionary mapping the 'Name' column to 'Spot' values
spot_prices = dict(zip(spot_df['Name'], spot_df['Price']))

# Calculate OrderAverageTradedPrice only for rows with missing values in that column
def calculate_order_average_traded_price(row):
    if pd.isna(row['OrderAverageTradedPrice']):
        symbol = row['SYMBOL']
        strike = row['STRIKE']
        option_type = row['OPTION TYPE']
        spot_price = spot_prices.get(symbol, 0)
        
        if option_type == 'CE':
            price = spot_price - strike
        elif option_type == 'PE':
            price = strike - spot_price
        else:
            price = 0
        
        return max(price, 0)
    else:
        return row['OrderAverageTradedPrice']

df['OrderAverageTradedPrice'] = df.apply(calculate_order_average_traded_price, axis=1)

print (len(df['OrderAverageTradedPrice'] == 0)) 

# **New Task**: Remove rows where 'OrderAverageTradedPrice' is 0
df = df[df['OrderAverageTradedPrice'] != 0]


# **New Modification 2**: Show specific columns for the last two rows of the final output file
print("\nLast two rows (specific columns) in the final output:")
columns_to_show = ["ClientID", "OrderQuantity", 
                   "OrderAverageTradedPrice", "INSTRUMENT NAME", 
                  "STRIKE","SYMBOL", "OPTION TYPE"]

# Display the last two rows with the selected columns
print(df[columns_to_show].tail(2))  # Show last two rows of selected columns

# Convert all values in the 'INSTRUMENT NAME' column to 'OPTIDX'
df['INSTRUMENT NAME'] = 'OPTIDX'

# Save the updated dataframe back to CSV
output_file_path = f"X:\\DATA WORK\\#Source\\#oderbook\\Updated_{input_date}_Orderbook.csv"

try:
    df.to_csv(output_file_path, index=False)
    print(f"File saved to {output_file_path}")
except Exception as e:
    print(f"Error saving file: {e}")
print(len(df))
