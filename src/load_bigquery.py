import pandas as pd
from google.cloud import bigquery
import io


def load_csv_to_bigquery(csv_path: str, project_id: str, dataset_id: str, table_id: str) -> None:
    """
    Loads a local CSV file into a BigQuery table (replaces the table each run).
    """
    client = bigquery.Client(project=project_id)

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Read CSV
    df = pd.read_csv(csv_path)

    df["home_score"] = df["home_score"].astype(int)
    df["away_score"] = df["away_score"].astype(int)
    df["run_diff"] = df["run_diff"].astype(int)
    df["total_runs"] = df["total_runs"].astype(int)
    df["month"] = df["month"].astype(int)

    # Explicit schema (good habit: avoids BigQuery guessing wrong types)
    schema = [
        bigquery.SchemaField("game_id", "STRING"),
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("season", "INTEGER"),
        bigquery.SchemaField("home_team", "STRING"),
        bigquery.SchemaField("away_team", "STRING"),
        bigquery.SchemaField("home_score", "INTEGER"),
        bigquery.SchemaField("away_score", "INTEGER"),
        bigquery.SchemaField("venue", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("run_diff", "INTEGER"),
        bigquery.SchemaField("total_runs", "INTEGER"),
        bigquery.SchemaField("winner_team", "STRING"),
        bigquery.SchemaField("is_one_run_game", "BOOLEAN"),
        bigquery.SchemaField("month", "INTEGER"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace table each run
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    # Convert game_date to ISO date strings so BigQuery DATE loads cleanly
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date.astype(str)

    # BigQuery expects lowercase booleans
    df["is_one_run_game"] = df["is_one_run_game"].astype(str).str.lower()

    # Load via dataframe -> CSV bytes in memory
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    load_job = client.load_table_from_file(
        file_obj=csv_buffer,
        destination=full_table_id,
        job_config=job_config,
    )

    load_job.result()  # wait for completion

    table = client.get_table(full_table_id)
    print(f"Loaded {table.num_rows} rows into {full_table_id}")


if __name__ == "__main__":
    CSV_PATH = "data/processed/games_clean.csv"
    PROJECT_ID = "mlb-data-pipeline-489220"
    DATASET_ID = "mlb_pipeline"
    TABLE_ID = "games_clean"

    load_csv_to_bigquery(CSV_PATH, PROJECT_ID, DATASET_ID, TABLE_ID)