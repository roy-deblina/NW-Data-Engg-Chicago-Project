package main

import (
    "database/sql"
    "encoding/csv"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/exec"
    _ "github.com/lib/pq" // Postgres driver
)

// HANDLER 1: TRIGGER THE AIRFLOW PIPELINE
func runPipelineHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
        return
    }
    log.Println("Received request to trigger pipeline...")

    // Command to trigger the Airflow DAG
    cmd := exec.Command("airflow", "dags", "trigger", "chicago_pipeline")
    
    if err := cmd.Run(); err != nil {
        log.Printf("Error triggering pipeline: %v", err)
        http.Error(w, "Failed to trigger pipeline", http.StatusInternalServerError)
        return
    }

    log.Println("Pipeline triggered successfully.")
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(`{"message": "Pipeline triggered successfully."}`))
}

// HANDLER 2: GET AIRPORT REPORT 
func airportReportHandler(w http.ResponseWriter, r *http.Request) {
    log.Println("Received request for airport report...")

    // 1. Get credentials from environment variables
    psqlInfo := fmt.Sprintf("host=%s user=%s password=%s dbname=chicago_bi sslmode=disable",
        os.Getenv("PG_HOST"),
        os.Getenv("PG_USER"),
        os.Getenv("PG_PASSWORD"),
    )

    // 2. Connect to the database
    db, err := sql.Open("postgres", psqlInfo)
    if err != nil {
        log.Printf("Error connecting to DB: %v", err)
        http.Error(w, "Database connection error", 500)
        return
    }
    defer db.Close()

    // 3. Define the SQL query for the report
    sqlQuery := `
        SELECT
            d.community_area_name AS origin,
            z.zip_code AS destination_zip,
            COUNT(t.trip_id) AS total_trips
        FROM
            fact_trips AS t
        JOIN
            dim_geography AS d ON t.pickup_geography_key = d.geography_key
        JOIN
            dim_geography AS z ON t.dropoff_geography_key = z.geography_key
        WHERE
            d.community_area_number IN (56, 76) -- 56=Midway, 76=O'Hare
        GROUP BY
            d.community_area_name, z.zip_code
        ORDER BY
            total_trips DESC
        LIMIT 100;
    `

    // 4. Run the query
    rows, err := db.Query(sqlQuery)
    if err != nil {
        log.Printf("Error running query: %v", err)
        http.Error(w, "Database query error", 500)
        return
    }
    defer rows.Close()

    // 5. Write the results back as a CSV file
    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-Disposition", "attachment;filename=airport_traffic_report.csv")
    
    csvWriter := csv.NewWriter(w)
    
    // Write CSV header
    cols, _ := rows.Columns()
    csvWriter.Write(cols)

    // Write CSV rows
    for rows.Next() {
        columns := make([]interface{}, len(cols))
        columnPointers := make([]interface{}, len(cols))
        for i := range columns {
            columnPointers[i] = &columns[i]
        }
        if err := rows.Scan(columnPointers...); err != nil {
            log.Printf("Error scanning row: %v", err)
        }
        rowStrings := make([]string, len(cols))
        for i, col := range columns {
            b, ok := col.([]byte)
            if ok {
                rowStrings[i] = string(b)
            } else {
                rowStrings[i] = "NULL"
            }
        }
        csvWriter.Write(rowStrings)
    }
    csvWriter.Flush()
    log.Println("Airport report sent.")
}

// MAIN FUNCTION: START THE SERVER 
func main() {
    http.HandleFunc("/run-pipeline", runPipelineHandler)
    http.HandleFunc("/reports/airport-traffic", airportReportHandler)
    
    // (You will add more /reports/... handlers here for your other reports)
    
    log.Println("Starting API server on port 80...")
    if err := http.ListenAndServe(":80", nil); err != nil {
        log.Fatal(err)
    }
}
