Here is the full, detailed, step-by-step guide for **Phase 3: Database Setup**.

These commands are all run from your **`api-vm`** terminal.

-----

### **Step 1: 🗄️ Create the Empty Database & Tables**

First, we will log in to your `db-vm` and create the "empty shell" of your Gold Zone.

1.  **Connect to your Postgres Server:**
    You should be logged into your `api-vm`. From its terminal, connect to your database VM (`db-vm`) using the `psql` (Postgres) client. It will use the environment variables you set up in Phase 2.

    ```bash
    psql -h $PG_HOST -U $PG_USER -d postgres
    ```

    It will ask for your Postgres password (Credential \#4).

2.  **Create Your Database:**
    Once you are in the `psql` shell (it will show `postgres=#`), create your database:

    ```sql
    CREATE DATABASE chicago_bi;
    ```

3.  **Connect to Your New Database:**

    ```sql
    \c chicago_bi
    ```

    Your prompt will change to `chicago_bi=#`.

4.  **Run the `CREATE TABLE` SQL Commands:**
    Copy and paste these SQL commands one by one. This builds your 6 empty Star Schema tables.

    ```sql
    -- 1. Create the 'dim_date' table
    CREATE TABLE dim_date (
        date_key INT PRIMARY KEY,
        full_date DATE NOT NULL,
        day_of_week VARCHAR(10),
        month_name VARCHAR(10),
        month_num INT,
        quarter INT,
        year INT
    );

    -- 2. Create the 'dim_geography' table (with PostGIS geom column)
    CREATE TABLE dim_geography (
        geography_key SERIAL PRIMARY KEY,
        zip_code VARCHAR(10),
        community_area_number INT,
        community_area_name VARCHAR(100),
        neighborhood_name VARCHAR(100),
        geom geometry(MultiPolygon, 4326) -- The PostGIS geometry column
    );

    -- 3. Create the 'fact_trips' table
    CREATE TABLE fact_trips (
        trip_id VARCHAR(255) PRIMARY KEY,
        trip_start_date_key INT REFERENCES dim_date(date_key),
        trip_end_date_key INT REFERENCES dim_date(date_key),
        pickup_geography_key INT REFERENCES dim_geography(geography_key),
        dropoff_geography_key INT REFERENCES dim_geography(geography_key),
        trip_source VARCHAR(10),
        trip_start_timestamp TIMESTAMPTZ,
        trip_end_timestamp TIMESTAMPTZ,
        trip_duration_seconds INT,
        trip_distance_miles NUMERIC(10, 2),
        fare NUMERIC(10, 2),
        tip NUMERIC(10, 2),
        trip_total NUMERIC(10, 2)
    );

    -- 4. Create the 'fact_permits' table
    CREATE TABLE fact_permits (
        permit_key VARCHAR(255) PRIMARY KEY,
        issue_date_key INT REFERENCES dim_date(date_key),
        geography_key INT REFERENCES dim_geography(geography_key),
        permit_type VARCHAR(100),
        work_description VARCHAR(1000),
        reported_cost NUMERIC(12, 2)
    );

    -- 5. Create the 'fact_covid_daily' table
    CREATE TABLE fact_covid_daily (
        covid_report_key VARCHAR(255) PRIMARY KEY,
        report_date_key INT REFERENCES dim_date(date_key),
        geography_key INT REFERENCES dim_geography(geography_key),
        cases_daily INT,
        cases_total INT
    );

    -- 6. Create the 'fact_health_socioeconomic' table
    CREATE TABLE fact_health_socioeconomic (
        health_report_key VARCHAR(255) PRIMARY KEY,
        report_year INT,
        geography_key INT REFERENCES dim_geography(geography_key),
        unemployment_rate NUMERIC(5, 2),
        poverty_rate NUMERIC(5, 2),
        ccvi_score NUMERIC(5, 3),
        ccvi_category VARCHAR(20)
    );
    ```

5.  **Exit the `psql` shell:**

    ```sql
    \q
    ```

    You are now back in your `api-vm`'s main terminal.

-----

### **Step 2: 📅 Populate the `dim_date` Table**

This script will fill your date "lookup" table.

1.  **Get the Python Script:**
    This script should be in the GitHub repo your team created. Make sure you have the latest version:

    ```bash
    cd ~/chicago-bi-project  # Or whatever your repo folder is named
    git pull
    ```

    If the file `populate_dates.py` doesn't exist, create it:

    ```bash
    nano populate_dates.py
    ```

    Paste this code into the editor. This script reads your environment variables to connect.

    ```python
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
    ```

    Press **Ctrl+O**, **Enter**, and **Ctrl+X** to save and exit.

2.  **Run the script:**

    ```bash
    python3 populate_dates.py
    ```

    It will connect to your database and fill the `dim_date` table.

-----

### **Step 3: 🗺️ Populate the `dim_geography` Table (The Core Task)**

This is the most complex step. You will download the 3 map files, load them into PostGIS, and join them.

1.  **Install `postgis` (client tools) and `unzip`:**
    You are on your `api-vm` terminal.

    ```bash
    sudo apt install postgis unzip -y
    ```

2.  **Download the 3 Shapefiles:**
    These are your 7 project datasets + 2 external. (Your 9 datasets are 7 project + 2 boundaries). We are downloading the boundary files.

    ```bash
    # Download Community Areas
    wget -O comm_areas.zip "https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=Shapefile"

    # Download ZIP Codes
    wget -O zip_codes.zip "https://data.cityofchicago.org/api/geospatial/gdcf-axmw?method=export&format=Shapefile"

    # Download Neighborhoods
    wget -O neighborhoods.zip "https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=Shapefile"
    ```

3.  **Unzip the Files:**

    ```bash
    unzip comm_areas.zip
    unzip zip_codes.zip
    unzip neighborhoods.zip
    ```

    (You can ignore any "inflating" messages).

4.  **Load Shapefiles into *Temporary* Postgres Tables:**
    This uses the `shp2pgsql` tool to convert the maps to SQL and pipe them directly into your database.

    ```bash
    # Load Community Areas (find the .shp file, it might be named 'Boundaries - Community Areas.shp')
    shp2pgsql -s 4326 -I "Boundaries - Community Areas (Maps).shp" temp_comm_areas | psql -h $PG_HOST -U $PG_USER -d chicago_bi

    # Load ZIP Codes
    shp2pgsql -s 4326 -I "Boundaries - ZIP Codes.shp" temp_zip_codes | psql -h $PG_HOST -U $PG_USER -d chicago_bi

    # Load Neighborhoods
    shp2pgsql -s 4326 -I "Boundaries - Neighborhoods.shp" temp_neighborhoods | psql -h $PG_HOST -U $PG_USER -d chicago_bi
    ```

    *(Note: You might need to adjust the `.shp` filenames. Use `ls` to see the exact names after unzipping.)*

5.  **Run the Final Spatial Join (The "Magic"):**

      * This query will build your master crosswalk. Log back into your database:
        ```bash
        psql -h $PG_HOST -U $PG_USER -d chicago_bi
        ```
      * Copy and paste this entire SQL query to populate your `dim_geography` table. It joins the 3 temp tables using their locations (`ST_Intersects`).
        ```sql
        INSERT INTO dim_geography (community_area_name, community_area_number, zip_code, neighborhood_name, geom)
        SELECT DISTINCT
            c.community AS community_area_name,
            CAST(c.area_num_1 AS INT) AS community_area_number,
            z.zip AS zip_code,
            n.nhood AS neighborhood_name,
            c.geom -- Use the Community Area geometry as the main one
        FROM
            temp_comm_areas AS c
        LEFT JOIN
            temp_zip_codes AS z ON ST_Intersects(c.geom, z.geom)
        LEFT JOIN
            temp_neighborhoods AS n ON ST_Intersects(c.geom, n.geom);
        ```

6.  **Clean Up:**
    While still in `psql`, drop the temporary tables you don't need anymore.

    ```sql
    DROP TABLE temp_comm_areas;
    DROP TABLE temp_zip_codes;
    DROP TABLE temp_neighborhoods;
    ```

7.  **Exit Postgres:**

    ```sql
    \q
    ```

### ✅ **You are now FINISHED with Phase 3\!**

Your "Gold Zone" database is now fully set up. The 6 tables are created, and your two critical dimension tables (`dim_date` and `dim_geography`) are full of data.

Your database is now ready to receive data from your ETL pipeline (Phase 4).
