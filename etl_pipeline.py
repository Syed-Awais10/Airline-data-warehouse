import pandas as pd
import requests
import pyodbc
import os
from urllib.parse import urlencode

# ===============================
# CONFIGURATION
# ===============================
from config import SERVER, DW_DB, CONN_STR, API_KEY, API_URL, CSV_PATH


# ===============================
# EXTRACT FUNCTIONS
# ===============================

def extract_oltp1(conn):
    print("🔹 Extracting from OLTP1...")
    df_customers = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP1.Customers", conn)
    df_bookings = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP1.Bookings", conn)
    df_payments = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP1.Payments", conn)
    return df_customers, df_bookings, df_payments

def extract_oltp2(conn):
    print("🔹 Extracting from OLTP2...")
    df_aircrafts = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP2.Aircrafts", conn)
    df_flights = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP2.Flights", conn)
    df_routes = pd.read_sql("SELECT * FROM AirFlightsOLTP.OLTP2.Routes", conn)
    return df_aircrafts, df_flights, df_routes

def extract_api():
    print("🔹 Extracting from AviationStack API...")
    try:
        params = {'access_key': API_KEY, 'limit': 100}
        url = f"{API_URL}?{urlencode(params)}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', [])
        return pd.json_normalize(data)
    except requests.exceptions.HTTPError as e:
        print(f"⚠️  API Error: {e}. Skipping API data extraction.")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️  Unexpected API error: {e}. Skipping API data extraction.")
        return pd.DataFrame()

def extract_csv():
    print("🔹 Reading CSV file...")
    df = pd.read_csv(CSV_PATH)
    return df

# ===============================
# TRANSFORM FUNCTIONS
# ===============================

def transform_oltp1_customers(df):
    print("🧪 Transforming Customers data...")
    # Standardize text fields
    if 'Name' in df.columns:
        df['Name'] = df['Name'].str.strip().str.title()
    # Remove duplicates based on CustomerID
    id_col = 'CustomerID' if 'CustomerID' in df.columns else df.columns[0]
    df = df.drop_duplicates(subset=[id_col], keep='first')
    return df

def transform_oltp1_bookings(df):
    print("🧪 Transforming Bookings data...")
    # Ensure proper data types
    if 'BookingDate' in df.columns:
        df['BookingDate'] = pd.to_datetime(df['BookingDate'], errors='coerce')
    # Remove any bookings with null CustomerID or FlightID
    required_cols = [col for col in ['CustomerID', 'FlightID'] if col in df.columns]
    if required_cols:
        df = df.dropna(subset=required_cols)
    # Remove duplicates
    id_col = 'BookingID' if 'BookingID' in df.columns else df.columns[0]
    df = df.drop_duplicates(subset=[id_col], keep='first')
    return df

def transform_oltp1_payments(df):
    print("🧪 Transforming Payments data...")
    # Standardize payment method text
    if 'PaymentMethod' in df.columns:
        df['PaymentMethod'] = df['PaymentMethod'].str.strip().str.title()
    # Ensure Amount is positive
    if 'Amount' in df.columns:
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        df = df[df['Amount'] > 0]
    # Convert PaymentDate to datetime
    if 'PaymentDate' in df.columns:
        df['PaymentDate'] = pd.to_datetime(df['PaymentDate'], errors='coerce')
    return df

def transform_oltp2_aircrafts(df):
    print("🧪 Transforming Aircrafts data...")
    # Standardize model names
    if 'Model' in df.columns:
        df['Model'] = df['Model'].str.strip().str.upper()
    # Ensure capacity is valid
    if 'Capacity' in df.columns:
        df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce')
        df = df[df['Capacity'] > 0]
    # Remove duplicates - use first column as ID if AircraftID doesn't exist
    id_col = None
    for possible_id in ['AircraftID', 'PlaneID', 'ID']:
        if possible_id in df.columns:
            id_col = possible_id
            break
    if id_col:
        df = df.drop_duplicates(subset=[id_col], keep='first')
    return df

def transform_oltp2_flights(df):
    print("🧪 Transforming Flights data...")
    # Standardize flight numbers
    if 'FlightNumber' in df.columns:
        df['FlightNumber'] = df['FlightNumber'].str.strip().str.upper()
    # Convert datetime columns
    if 'DepartureTime' in df.columns:
        df['DepartureTime'] = pd.to_datetime(df['DepartureTime'], errors='coerce')
    if 'ArrivalTime' in df.columns:
        df['ArrivalTime'] = pd.to_datetime(df['ArrivalTime'], errors='coerce')
    # Remove duplicates
    id_col = 'FlightID' if 'FlightID' in df.columns else df.columns[0]
    df = df.drop_duplicates(subset=[id_col], keep='first')
    return df

def transform_oltp2_routes(df):
    print("🧪 Transforming Routes data...")
    # Standardize location names
    if 'Origin' in df.columns:
        df['Origin'] = df['Origin'].str.strip().str.title()
    if 'Destination' in df.columns:
        df['Destination'] = df['Destination'].str.strip().str.title()
    # Ensure distance is valid
    if 'Distance' in df.columns:
        df['Distance'] = pd.to_numeric(df['Distance'], errors='coerce')
        df = df[df['Distance'] > 0]
    # Remove duplicates
    id_col = 'RouteID' if 'RouteID' in df.columns else df.columns[0]
    df = df.drop_duplicates(subset=[id_col], keep='first')
    return df

def transform_api(df):
    print("🧪 Transforming API data...")
    if df.empty:
        return df
    # Flatten nested JSON structures if they exist
    # Standardize column names (API might have different naming)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    # Remove duplicates based on flight identifier
    if 'flight_iata' in df.columns:
        df = df.drop_duplicates(subset=['flight_iata'], keep='first')
    return df

def transform_csv(df):
    print("🧪 Transforming CSV data...")
    # Drop unnamed index columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # Standardize satisfaction column
    if 'satisfaction' in df.columns:
        df['Satisfaction'] = df['satisfaction'].str.title()
    
    # Calculate average rating from service columns
    rating_cols = [
        'Inflight wifi service', 'Departure/Arrival time convenient', 'Ease of Online booking',
        'Gate location', 'Food and drink', 'Online boarding', 'Seat comfort',
        'Inflight entertainment', 'On-board service', 'Leg room service', 'Baggage handling',
        'Checkin service', 'Inflight service', 'Cleanliness'
    ]
    existing_cols = [col for col in rating_cols if col in df.columns]
    if existing_cols:
        df['AverageRating'] = df[existing_cols].mean(axis=1)
    
    # Map CSV columns to database columns for Stg_CustomerSatisfaction
    # Expected columns: CustomerID, TypeOfTravel, Class, FlightDistance, Satisfaction, AverageRating
    column_mapping = {
        'id': 'CustomerID',
        'Type of Travel': 'TypeOfTravel',
        'Class': 'Class',
        'Flight Distance': 'FlightDistance',
        'Satisfaction': 'Satisfaction',
        'AverageRating': 'AverageRating'
    }
    
    # Rename columns to match database schema
    df_mapped = pd.DataFrame()
    for csv_col, db_col in column_mapping.items():
        if csv_col in df.columns:
            df_mapped[db_col] = df[csv_col]
    
    # Ensure all required columns exist
    required_cols = ['CustomerID', 'TypeOfTravel', 'Class', 'FlightDistance', 'Satisfaction', 'AverageRating']
    for col in required_cols:
        if col not in df_mapped.columns:
            print(f"⚠️  Warning: Column '{col}' not found in CSV. Adding with NULL values.")
            df_mapped[col] = None
    
    # Select only the required columns in the correct order
    df_final = df_mapped[required_cols]
    
    return df_final

# ===============================
# LOAD FUNCTIONS
# ===============================

def get_table_columns(conn, table_name):
    """Query the database to get actual column names for a table"""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def load_to_staging(conn, df, table_name):
    print(f"📥 Loading {table_name} into Staging...")
    cursor = conn.cursor()
    
    # Get actual table columns from database
    db_columns = get_table_columns(conn, table_name)
    
    if not db_columns:
        print(f"⚠️  Warning: Could not retrieve columns for {table_name}. Attempting direct insert...")
    else:
        # Filter dataframe to only include columns that exist in the database table
        valid_columns = [col for col in df.columns if col in db_columns]
        if len(valid_columns) != len(df.columns):
            missing = set(df.columns) - set(db_columns)
            print(f"⚠️  Warning: Columns {missing} not found in {table_name}. Skipping these columns.")
        df = df[valid_columns]
    
    # Clear staging table
    cursor.execute(f"DELETE FROM dbo.{table_name}")
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        cols = ', '.join([f'[{col}]' for col in row.index])
        placeholders = ', '.join(['?'] * len(row))
        sql = f"INSERT INTO dbo.{table_name} ({cols}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, tuple(row))
        except Exception as e:
            print(f"❌ Error inserting row: {e}")
            print(f"   Row data: {row.to_dict()}")
            raise
    
    conn.commit()
    print(f"✅ {len(df)} rows loaded into {table_name}")

def print_table_info(conn, table_name):
    """Print columns of a table for debugging"""
    columns = get_table_columns(conn, table_name)
    print(f"📋 {table_name} columns: {', '.join(columns)}")
    return columns

def merge_to_dim_fact(conn):
    print("🔄 Merging Staging → Dimensions and Fact Tables...")

    cursor = conn.cursor()
    
    # Print staging table structures for debugging
    print("\n📊 Staging Table Structures:")
    stg_customers_cols = print_table_info(conn, 'Stg_Customers')
    stg_aircrafts_cols = print_table_info(conn, 'Stg_Aircrafts')
    stg_routes_cols = print_table_info(conn, 'Stg_Routes')
    stg_flights_cols = print_table_info(conn, 'Stg_Flights')
    stg_payments_cols = print_table_info(conn, 'Stg_Payments')
    stg_satisfaction_cols = print_table_info(conn, 'Stg_CustomerSatisfaction')
    stg_bookings_cols = print_table_info(conn, 'Stg_Bookings')
    print()
    
    # Merge DimCustomer
    try:
        cursor.execute("""
            MERGE dbo.DimCustomer AS tgt
            USING dbo.Stg_Customers AS src
            ON tgt.CustomerID = src.CustomerID
            WHEN MATCHED THEN UPDATE SET tgt.Name = src.Name
            WHEN NOT MATCHED THEN INSERT (CustomerID, Name) VALUES (src.CustomerID, src.Name);
        """)
        print("✅ DimCustomer merged")
    except Exception as e:
        print(f"❌ Error merging DimCustomer: {e}")

    # Merge DimAircraft
    try:
        aircraft_id_col = 'PlaneID' if 'PlaneID' in stg_aircrafts_cols else 'AircraftID'
        cursor.execute(f"""
            MERGE dbo.DimAircraft AS tgt
            USING dbo.Stg_Aircrafts AS src
            ON tgt.AircraftID = src.{aircraft_id_col}
            WHEN NOT MATCHED THEN
            INSERT (AircraftID, Model, Capacity, ManufactureYear)
            VALUES (src.{aircraft_id_col}, src.Model, src.Capacity, src.ManufactureYear);
        """)
        print("✅ DimAircraft merged")
    except Exception as e:
        print(f"❌ Error merging DimAircraft: {e}")

    # Merge DimRoute
    try:
        route_id_col = 'RouteID' if 'RouteID' in stg_routes_cols else stg_routes_cols[0]
        # Map Source->Origin in staging table
        origin_col = 'Source' if 'Source' in stg_routes_cols else 'Origin'
        cursor.execute(f"""
            MERGE dbo.DimRoute AS tgt
            USING dbo.Stg_Routes AS src
            ON tgt.RouteID = src.{route_id_col}
            WHEN NOT MATCHED THEN
            INSERT (RouteID, Origin, Destination, Distance)
            VALUES (src.{route_id_col}, src.{origin_col}, src.Destination, src.Distance);
        """)
        print("✅ DimRoute merged")
    except Exception as e:
        print(f"❌ Error merging DimRoute: {e}")

    # Merge DimFlight
    try:
        flight_id_col = 'FlightID' if 'FlightID' in stg_flights_cols else stg_flights_cols[0]
        # Stg_Flights doesn't have FlightNumber, so we'll use FlightID as FlightNumber
        cursor.execute(f"""
            MERGE dbo.DimFlight AS tgt
            USING dbo.Stg_Flights AS src
            ON tgt.FlightID = src.{flight_id_col}
            WHEN NOT MATCHED THEN
            INSERT (FlightID, FlightNumber, DepartureTime, ArrivalTime)
            VALUES (src.{flight_id_col}, CAST(src.{flight_id_col} AS NVARCHAR(50)), src.DepartureTime, src.ArrivalTime);
        """)
        print("✅ DimFlight merged")
    except Exception as e:
        print(f"❌ Error merging DimFlight: {e}")

    # Merge DimPayment
    try:
        payment_id_col = 'PaymentID' if 'PaymentID' in stg_payments_cols else stg_payments_cols[0]
        # Map Method->PaymentMethod, Date->PaymentDate
        method_col = 'Method' if 'Method' in stg_payments_cols else 'PaymentMethod'
        date_col = 'Date' if 'Date' in stg_payments_cols else 'PaymentDate'
        cursor.execute(f"""
            MERGE dbo.DimPayment AS tgt
            USING dbo.Stg_Payments AS src
            ON tgt.PaymentID = src.{payment_id_col}
            WHEN NOT MATCHED THEN
            INSERT (PaymentID, PaymentMethod, Amount, PaymentDate)
            VALUES (src.{payment_id_col}, src.{method_col}, src.Amount, src.{date_col});
        """)
        print("✅ DimPayment merged")
    except Exception as e:
        print(f"❌ Error merging DimPayment: {e}")

    # Merge DimSatisfaction
    try:
        satisfaction_id_col = 'CustomerID' if 'CustomerID' in stg_satisfaction_cols else stg_satisfaction_cols[0]
        # Stg_CustomerSatisfaction doesn't have AverageRating, calculate it or use NULL
        cursor.execute(f"""
            MERGE dbo.DimSatisfaction AS tgt
            USING (
                SELECT 
                    CustomerID,
                    TypeOfTravel,
                    Class,
                    FlightDistance,
                    Satisfaction,
                    (CAST(InflightWifiService AS FLOAT) + 
                     CAST(DepartureArrivalConvenient AS FLOAT) + 
                     CAST(EaseOfOnlineBooking AS FLOAT) + 
                     CAST(GateLocation AS FLOAT) + 
                     CAST(FoodAndDrink AS FLOAT) + 
                     CAST(OnlineBoarding AS FLOAT) + 
                     CAST(SeatComfort AS FLOAT) + 
                     CAST(InflightEntertainment AS FLOAT) + 
                     CAST(OnboardService AS FLOAT) + 
                     CAST(LegRoomService AS FLOAT) + 
                     CAST(BaggageHandling AS FLOAT) + 
                     CAST(CheckinService AS FLOAT) + 
                     CAST(InflightService AS FLOAT) + 
                     CAST(Cleanliness AS FLOAT)) / 14.0 AS AverageRating
                FROM dbo.Stg_CustomerSatisfaction
            ) AS src
            ON tgt.CustomerID = src.CustomerID
            WHEN NOT MATCHED THEN
            INSERT (CustomerID, TypeOfTravel, Class, FlightDistance, Satisfaction, AverageRating)
            VALUES (src.CustomerID, src.TypeOfTravel, src.Class, src.FlightDistance, src.Satisfaction, src.AverageRating);
        """)
        print("✅ DimSatisfaction merged")
    except Exception as e:
        print(f"❌ Error merging DimSatisfaction: {e}")

    # Merge FactBooking
    try:
        booking_id_col = 'BookingID' if 'BookingID' in stg_bookings_cols else stg_bookings_cols[0]
        customer_id_col = 'CustomerID' if 'CustomerID' in stg_bookings_cols else 'CustomerID'
        flight_id_col = 'FlightID' if 'FlightID' in stg_bookings_cols else 'FlightID'
        date_col = 'Date' if 'Date' in stg_bookings_cols else 'BookingDate'
        
        # Get RouteID and PlaneID from Stg_Flights, not Stg_Bookings
        cursor.execute(f"""
            MERGE dbo.FactBooking AS tgt
            USING (
                SELECT 
                    b.{booking_id_col} AS BookingID,
                    b.{customer_id_col} AS CustomerID,
                    b.{flight_id_col} AS FlightID,
                    f.RouteID,
                    f.PlaneID AS AircraftID,
                    b.{booking_id_col} AS PaymentID,
                    b.{customer_id_col} AS SatisfactionKey,
                    b.{date_col} AS BookingDate
                FROM dbo.Stg_Bookings b
                LEFT JOIN dbo.Stg_Flights f ON b.{flight_id_col} = f.FlightID
            ) AS src
            ON tgt.BookingID = src.BookingID
            WHEN NOT MATCHED THEN
            INSERT (BookingID, CustomerID, FlightID, RouteID, AircraftID, PaymentID, SatisfactionKey, BookingDate)
            VALUES (src.BookingID, src.CustomerID, src.FlightID, src.RouteID, src.AircraftID, src.PaymentID, src.SatisfactionKey, src.BookingDate);
        """)
        print("✅ FactBooking merged")
    except Exception as e:
        print(f"❌ Error merging FactBooking: {e}")

    conn.commit()
    print("\n✅ Merge complete!")

# ===============================
# MAIN
# ===============================

def main():
    conn = pyodbc.connect(CONN_STR)

    # Extract from OLTP databases
    df_cust, df_book, df_pay = extract_oltp1(conn)
    df_air, df_flights, df_routes = extract_oltp2(conn)
    
    # Extract from API and CSV
    df_api = extract_api()
    df_csv = extract_csv()

    # Transform all data sources
    df_cust = transform_oltp1_customers(df_cust)
    df_book = transform_oltp1_bookings(df_book)
    df_pay = transform_oltp1_payments(df_pay)
    df_air = transform_oltp2_aircrafts(df_air)
    df_flights = transform_oltp2_flights(df_flights)
    df_routes = transform_oltp2_routes(df_routes)
    df_api = transform_api(df_api)
    df_csv = transform_csv(df_csv)

    # Load to staging tables
    load_to_staging(conn, df_cust, 'Stg_Customers')
    load_to_staging(conn, df_book, 'Stg_Bookings')
    load_to_staging(conn, df_pay, 'Stg_Payments')
    load_to_staging(conn, df_air, 'Stg_Aircrafts')
    load_to_staging(conn, df_flights, 'Stg_Flights')
    load_to_staging(conn, df_routes, 'Stg_Routes')
    load_to_staging(conn, df_csv, 'Stg_CustomerSatisfaction')
    
    # Only load API data if we got any
    if not df_api.empty:
        load_to_staging(conn, df_api, 'Stg_API_Flights')
    else:
        print("⏭️  Skipping Stg_API_Flights (no data from API)")

    # Merge to dimension and fact tables
    merge_to_dim_fact(conn)

    conn.close()
    print("🎯 ETL Pipeline completed successfully!")

# ===============================
# RUN
# ===============================

if __name__ == "__main__":
    main()