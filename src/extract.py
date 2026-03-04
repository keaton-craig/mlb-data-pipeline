import requests
import pandas as pd


def get_games(start_date, end_date):
    """
    Pull MLB game schedule data from the MLB Stats API.
    """

    url = "https://statsapi.mlb.com/api/v1/schedule"

    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date
    }

    response = requests.get(url, params=params)
    data = response.json()

    games = []

    for date in data.get("dates", []):
        for game in date.get("games", []):

            games.append({
                "game_id": game["gamePk"],
                "game_date": date["date"],
                "season": game["season"],
                "home_team": game["teams"]["home"]["team"]["name"],
                "away_team": game["teams"]["away"]["team"]["name"],
                "home_score": game["teams"]["home"].get("score"),
                "away_score": game["teams"]["away"].get("score"),
                "venue": game["venue"]["name"],
                "status": game["status"]["detailedState"]
            })

    return pd.DataFrame(games)


if __name__ == "__main__":

    df = get_games("2024-03-20", "2024-09-30")

    print(df.head())
    print(len(df), "games pulled")

# Save raw data
output_path = "data/raw/mlb_games_raw.csv"
df.to_csv(output_path, index=False)

print(f"Raw data saved to {output_path}")