import pandas as pd


def transform_games(input_path, output_path):
    """
    Clean and transform MLB game data.
    """

    # Load raw data
    df = pd.read_csv(input_path)

    # Convert data types
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce")
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce")

    # Remove duplicate games
    df = df.drop_duplicates(subset=["game_id"])

    # Keep only completed games
    df = df[df["status"] == "Final"]

    # Create derived columns
    df["run_diff"] = df["home_score"] - df["away_score"]
    df["total_runs"] = df["home_score"] + df["away_score"]

    df["winner_team"] = df.apply(
        lambda row: row["home_team"]
        if row["home_score"] > row["away_score"]
        else row["away_team"],
        axis=1
    )

    df["is_one_run_game"] = abs(df["run_diff"]) == 1
    df["month"] = df["game_date"].dt.month

    # Save cleaned dataset
    df.to_csv(output_path, index=False)

    print(f"Clean dataset saved to {output_path}")
    print(f"{len(df)} cleaned games")


if __name__ == "__main__":

    input_path = "data/raw/mlb_games_raw.csv"
    output_path = "data/processed/games_clean.csv"

    transform_games(input_path, output_path)