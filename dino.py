from flask import Flask, request, render_template, redirect
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import logging
import datetime

app = Flask(__name__)
logger = logging.getLogger(__name__)

# MySQL connection setup
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='Susi@1219',
        database='stores_db'
    )

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017')
db = client['dino_reports']
bills_collection = db['bills']

def fetch_stores():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        # Fetch stores from the database
        cursor.execute("SELECT id, name, cin FROM stores WHERE enable_arms_integration = 1")
        stores = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    return stores

def convert_epoch_to_human_readable(epoch_timestamp):
    date_time = datetime.datetime.fromtimestamp(epoch_timestamp)
    formatted_date = date_time.strftime('%Y-%m-%d')
    return formatted_date

@app.route('/', methods=['GET', 'POST'])
def index():
    stores = fetch_stores()
    # Convert the stores list for rendering
    stores_list = [(store['id'], f"{store['name']} - {store['cin']}") for store in stores]

    if request.method == 'POST':
        # Extractdata from the form
        store_id = request.form.get('store_dropdown')
        from_date_picker = request.form.get('from_date_picker')
        to_date_picker = request.form.get('to_date_picker')

        # Check if store_id is provided and valid
        if not store_id:
            logger.debug("Store ID not provided.")
            return render_template('index.html', stores=stores_list, message="Store ID not provided.")

        logger.debug("Form data: store_id: %s, from_date_picker: %s, to_date_picker: %s", store_id, from_date_picker, to_date_picker)
    
         # Handle file upload
        file = request.files.get('source_file')
        if not file:
            logger.debug("No CSV file uploaded.")
            return render_template('index.html', stores=stores_list, message="No CSV file uploaded.")
         
        # Read the CSV file into a Pandas DataFrame
        csv_df = pd.read_csv(file)

        # Sort DataFrame by 'date' column in ascending order
        csv_df = csv_df.sort_values(by='bill_date', ascending=True)
        logger.debug("CSV DataFrame loaded and filtered.")

        if csv_df.empty:
            logger.debug("No records found in the CSV file for the selected date range.")
            return render_template('index.html', stores=stores_list, message="No records found in the CSV file for the selected date range.")
        
        # Fetch the MongoDB store ID corresponding to the selected store ID
        selected_store = next((store for store in stores if store['id'] == int(store_id)), None)

        if not selected_store:
            logger.debug("No MongoDB store ID found for the selected store.")
            return render_template('index.html', stores=stores_list, message="No MongoDB store ID found for the selected store.")

        mongo_store_id = selected_store['id']
     
        # Convert date picker inputs to datetime objects
        from_date = pd.to_datetime(from_date_picker, format='%Y-%m-%d')
        to_date = pd.to_datetime(to_date_picker, format='%Y-%m-%d')

        from_date_epoch = int(from_date.timestamp())
        from_date = from_date.replace(hour=0, minute=0, second=0)
        to_date = to_date.replace(hour=23, minute=59, second=59)
        to_date_epoch = int(to_date.timestamp())

        bills_query = {
            'store_id': mongo_store_id,
            'bill_date': {
                '$gte': from_date_epoch,
                '$lte': to_date_epoch
            }
        }

        projection_fields = {
            'bill_date': 1,
            'bill_num': 1,
            'payble_amt': 1,
            'tax_amt': 1,
        }

        # Fetch data from the bills collection
        bills_data_cursor = bills_collection.find(bills_query, projection_fields)
 
        # Filter CSV data based on the date range
        # Convert from_date and to_date to timestamps
        csv_df['bill_date'] = pd.to_datetime(csv_df['bill_date'])
        from_date = pd.to_datetime(from_date)
        to_date = pd.to_datetime(to_date)
        csv_df = csv_df[(csv_df['bill_date'] >= from_date) & (csv_df['bill_date'] <= to_date)]

        # for bill in bills_data_cursor:
        #     print(bill)
        
        # Convert the cursor to a DataFrame
        bills_df = pd.DataFrame(list(bills_data_cursor))
        bills_df['taxable_amount'] = bills_df['payble_amt'] - bills_df['tax_amt']
        logger.debug("MongoDB DataFrame loaded.")

        if 'bill_date' not in bills_df.columns:
            logger.debug("No 'bill_date' column found in the bills collection data.")
            return render_template('index.html', stores=stores_list, message="No 'bill_date' column found in the bills collection data.")

        if bills_df.empty:
            logger.debug("No records found for the given bill_date range and store.")
            return render_template('index.html', stores=stores_list, message="No records found for the given date range and store.")
 
        # Merge CSV data and bills data based on date and bill_num
        csv_df['bill_num'] = csv_df['bill_num'].astype('str')
        bills_df['bill_num'] = bills_df['bill_num'].astype('str')

        differences = csv_df.merge(bills_df, on=['bill_num'], how='inner')

        # Calculate differences between CSV data and bills data
        differences['Bill_Difference'] = differences['gross'] - differences['payble_amt']
        differences['Tax_Difference'] = differences['tax'] - differences['tax_amt']
        differences['Net_Difference'] = differences['net'] - differences['taxable_amount']

        # Add store_name and cin columns to the differences DataFrame
        differences['store_name'] = selected_store['name']
        differences['cin'] = selected_store['cin']

        # Reset index for both DataFrames before concatenation
        differences.reset_index(drop=True, inplace=True)
        csv_df.reset_index(drop=True, inplace=True)

        # Extract the desired columns from the differences DataFrame
        selected_columns_df1 = differences[['store_name', 'cin', 'gross', 'payble_amt', 'tax', 'tax_amt', 'net', 'taxable_amount', 'Bill_Difference', 'Tax_Difference', 'Net_Difference']]
        selected_columns_df2 = csv_df[['bill_date', 'bill_num']]

        final_selected_df = selected_columns_df1.join(selected_columns_df2)

        final_selected_df.reset_index(drop=True, inplace=True)
        final_selected_df['SerialNumber'] = final_selected_df.index + 1

        final_selected_df = final_selected_df.fillna(value='')
        final_selected_df = final_selected_df.round(2)
        numeric_columns = ['gross', 'payble_amt', 'net', 'taxable_amount', 'tax', 'tax_amt', 'Bill_Difference', 'Tax_Difference', 'Net_Difference']
        final_selected_df[numeric_columns] = final_selected_df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        total_row = {
            'SerialNumber': 'Total',
            'gross': round(final_selected_df['gross'].sum(), 2),
            'payble_amt': round(final_selected_df['payble_amt'].sum(), 2),
            'net': round(final_selected_df['net'].sum(), 2),
            'taxable_amount': round(final_selected_df['taxable_amount'].sum(), 2),
            'tax': round(final_selected_df['tax'].sum(), 2),
            'tax_amt': round(final_selected_df['tax_amt'].sum(), 2),
            'Bill_Difference': round(final_selected_df['Bill_Difference'].sum(), 2),
            'Tax_Difference': round(final_selected_df['Tax_Difference'].sum(), 2),
            'Net_Difference': round(final_selected_df['Net_Difference'].sum(), 2)
        }

        total_df = pd.DataFrame(total_row, index=[0])
        total_df.fillna(0, inplace=True)
        final_selected_df = final_selected_df.fillna('')

        final_selected_df = pd.concat([final_selected_df, total_df], ignore_index=True)
         # Example handling NaN values by replacing them with zeros:
        final_selected_df = final_selected_df.fillna('')
        final_selected_df['bill_date'] = final_selected_df['bill_date'].apply(lambda x: '' if pd.isnull(x) else x)

        data_to_render = final_selected_df.to_dict('records')
        return render_template('result.html', data=data_to_render)

    return render_template('index.html', stores=stores_list)

if __name__ == '__main__':
    app.run(debug=True)


       