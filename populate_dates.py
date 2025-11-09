import os
import psycopg2
import datetime

def populate_dates():
    # Get database credentials from environment variables
    conn_string = f"dbname='chicago_bi' user='{os.environ.get('PG_USER')}' " \
                  f"password='{os.environ.get('PG_PASSWORD')}' host='{os.environ.get('PG_HOST')}'"
    
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print("Successfully connected to PostgreSQL.")

        start_date = datetime.date(2010, 1, 1)
        end_date = datetime.date(2030, 12, 31)
        current_date = start_date

        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            full_date = current_date
            day_of_week = current_date.strftime('%A')
            month_name = current_date.strftime('%B')
            month_num = current_date.month
            quarter = (current_date.month - 1) // 3 + 1
            year = current_date.year

            # Insert the data
            cursor.execute(
                """
                INSERT INTO dim_date (date_key, full_date, day_of_week, month_name, month_num, quarter, year)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING;
                """,
                (date_key, full_date, day_of_week, month_name, month_num, quarter, year)
            )
            
            current_date += datetime.timedelta(days=1)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully populated dim_date from {start_date} to {end_date}.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    populate_dates()
    