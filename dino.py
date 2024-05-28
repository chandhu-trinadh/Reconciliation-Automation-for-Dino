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

@app.route('/', methods=['GET', 'POST'])
def index():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        # Fetch stores from the database
        cursor.execute("SELECT id, name, cin FROM stores WHERE enable_arms_integration = 1")
        stores = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

    # Convert the stores list for rendering
    stores_list = [(store['id'], f"{store['name']} - {store['cin']}") for store in stores]

    if request.method == 'POST':
        # Extract data from the form
        store_id = request.form.get('store_dropdown')
        from_date_picker = request.form.get('from_date_picker')
        to_date_picker = request.form.get('to_date_picker')

        # Check if store_id is provided and valid
        if not store_id:
            logger.debug("Store ID not provided.")
            return render_template('index.html', stores=stores_list, message="Store ID not provided.")

        # Log form data for debugging
        logger.debug("Form data: store_id: %s, from_date_picker: %s, to_date_picker: %s", store_id, from_date_picker, to_date_picker)

        # Handle file upload
        file = request.files.get('source_file')
        if not file:
            logger.debug("No CSV file uploaded.")
            return render_template('index.html', stores=stores_list, message="No CSV file uploaded.")

        # Read the CSV file into a Pandas DataFrame
        csv_df = pd.read_csv(file)

        # Rename columns as required

        # Sort DataFrame by 'date' column in ascending order
        csv_df = csv_df.sort_values(by='bill_date', ascending=True)
        logger.debug("CSV DataFrame loaded and filtered.")

        if csv_df.empty:
            logger.debug("No records found in the CSV file for the selected date range.")
            return render_template('index.html', stores=stores_list, message="No records found in the CSV file for the selected date range.")

        # Fetch the MongoDB store ID corresponding to the selected store ID
        selected_store = next((store for store in stores if store['id'] == int(store_id)), None)
        # print(selected_store)

        if not selected_store:
            logger.debug("No MongoDB store ID found for the selected store.")
            return render_template('index.html', stores=stores_list, message="No MongoDB store ID found for the selected store.")

        mongo_store_id = selected_store['id']
        # print(mongo_store_id)

        # Convert date picker inputs to datetime objects
        from_date = pd.to_datetime(from_date_picker, format='%Y-%m-%d')
        to_date = pd.to_datetime(to_date_picker, format='%Y-%m-%d')
        
        def convert_epoch_to_human_readable(epoch_timestamp):
            """
            Convert an epoch timestamp to a human-readable date string.
    
               :param epoch_timestamp: Epoch timestamp (int)
               :return: Formatted date string (str)
            """
    # Convert epoch to datetime
            date_time = datetime.datetime.fromtimestamp(epoch_timestamp)
    
    # Format the datetime object into a string format, e.g., 'YYYY-MM-DD HH:MM:SS'
            formatted_date = date_time.strftime('%Y-%m-%d')
    
            return formatted_date
        # Convert epoch to date format for MongoDB query
        from_date_epoch = int(from_date.timestamp())
        to_date = to_date.replace(hour=23, minute=59, second=59) 
        to_date_epoch = int(to_date.timestamp())
        
# Convert the epoch timestamps to human-readable format
        # from_date_str = convert_epoch_to_human_readable(from_date_epoch)
        # to_date_str = convert_epoch_to_human_readable(to_date_epoch)

        # Query the bills collection for data related to the store ID and date range
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
       
        # # Fetch data from the bills collection
        bills_data_cursor = bills_collection.find(bills_query, projection_fields)
        # print(bills_data_cursor)

        # for bill in bills_data_cursor:
        #     print(bill)
        
        # Convert the cursor to a DataFrame
        bills_df = pd.DataFrame(list(bills_data_cursor))
        bills_df['taxable_amount'] = bills_df['payble_amt'] - bills_df['tax_amt']
        # print(bills_df['taxable_amount'])
        # print(bills_df)
        
        logger.debug("MongoDB DataFrame loaded.")

        # Ensure the 'date' column exists
        if 'bill_date' not in bills_df.columns:
            logger.debug("No 'bill_date' column found in the bills collection data.")
            return render_template('index.html', stores=stores_list, message="No 'bill_date' column found in the bills collection data.")

        # If no records are found
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
        # print(differences)

        # Reset index for both DataFrames before concatenation
        differences.reset_index(drop=True, inplace=True)
        csv_df.reset_index(drop=True, inplace=True)

        # Extract the desired columns from the differences DataFrame
        selected_columns_df1 = differences[['store_name', 'cin', 'gross', 'payble_amt','tax', 'tax_amt', 'net', 'taxable_amount',  'Bill_Difference', 'Tax_Difference', 'Net_Difference']]
        selected_columns_df2 = csv_df[['bill_date', 'bill_num']]
        
        # Concatenate selected_columns_df1 and selected_columns_df2 column-wise
        final_selected_df = selected_columns_df1.join(selected_columns_df2)

        # Add a serial number column by resetting the index
        final_selected_df.reset_index(drop=True, inplace=True)
        final_selected_df['SerialNumber'] = final_selected_df.index + 1  # Start with 1 for all rows

        # Replace NaN values with empty strings
        final_selected_df = final_selected_df.fillna(value='')

        # Round off float values to two decimal places
        final_selected_df = final_selected_df.round(2)

        # Calculate total for each column
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
       
        # Example handling NaN values by replacing them with zeros:
        total_df.fillna(0, inplace=True)
        final_selected_df = final_selected_df.fillna('')

         # Append total row to DataFrame
        final_selected_df = pd.concat([final_selected_df,total_df], ignore_index=True)
        final_selected_df = final_selected_df.fillna('')
         
        # Convert final_selected_df to dictionary for rendering
        data_to_render = final_selected_df.to_dict('records')
        # print(data_to_render)
            
        return render_template('result.html', data=data_to_render)
            
    return render_template('index.html', stores=stores_list)

if __name__ == '__main__':
    app.run(debug=True)


       