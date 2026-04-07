"""Pipeline ETL para Previsoes League.

Extrai dados da API-Football, transforma e carrega no PostgreSQL.
Foco exclusivo na Premier League 2024.

Uso:
    python -m etl.etl

Configuracao:
    - LEAGUE_IDS: Premier League (ID: 39)
    - SEASON: Temporada 2024
    - MAX_TEAMS: Maximo de times por liga (controle de rate limit)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_cursor, init_database, test_connection
from services.api_client import APIFootballClient

# Configuracao - Apenas Premier League
LEAGUE_IDS = [
    39,   # Premier League (Inglaterra)
]
SEASON = 2024
MAX_TEAMS = 20  # Max times por liga

LEAGUE_NAMES = {
    39: "Premier League",
}


def upsert_team(cursor, team_data: dict, league_id: int,
                league_name: str, season: int) -> int:
    """Insert or update a team and return the internal database ID.

    Args:
        cursor: Database cursor.
        team_data: Team information from the API.
        league_id: API league ID.
        league_name: Name of the league.
        season: Season year.

    Returns:
        Internal database ID of the team.
    """
    team = team_data.get("team", team_data)
    api_id = team["id"]
    name = team["name"]
    logo = team.get("logo", "")
    country = team.get("country", "")

    cursor.execute(
        """
        INSERT INTO teams (api_id, name, logo, country, league_id, league_name, season)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (api_id) DO UPDATE SET
            name = EXCLUDED.name,
            logo = EXCLUDED.logo,
            country = EXCLUDED.country,
            league_id = EXCLUDED.league_id,
            league_name = EXCLUDED.league_name,
            season = EXCLUDED.season,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
        """,
        (api_id, name, logo, country, league_id, league_name, season),
    )
    return cursor.fetchone()["id"]


def upsert_player(cursor, player_info: dict, team_db_id: int) -> int:
    """Insert or update a player and return the internal database ID.

    Args:
        cursor: Database cursor.
        player_info: Player information from the API.
        team_db_id: Internal database ID of the player's team.

    Returns:
        Internal database ID of the player.
    """
    api_id = player_info["id"]
    name = player_info.get("name", "")
    firstname = player_info.get("firstname", "")
    lastname = player_info.get("lastname", "")
    age = player_info.get("age")
    nationality = player_info.get("nationality", "")
    height = player_info.get("height", "")
    weight = player_info.get("weight", "")
    photo = player_info.get("photo", "")

    cursor.execute(
        """
        INSERT INTO players (api_id, name, firstname, lastname, age,
                            nationality, height, weight, photo, team_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (api_id) DO UPDATE SET
            name = EXCLUDED.name,
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            age = EXCLUDED.age,
            nationality = EXCLUDED.nationality,
            height = EXCLUDED.height,
            weight = EXCLUDED.weight,
            photo = EXCLUDED.photo,
            team_id = EXCLUDED.team_id,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
        """,
        (api_id, name, firstname, lastname, age, nationality,
         height, weight, photo, team_db_id),
    )
    return cursor.fetchone()["id"]


def extract_stat(stat: dict, key: str, subkey: str, default=0):
    """Safely extract a nested stat value from the API response.

    Args:
        stat: Statistics dict from the API.
        key: Top-level key (e.g., 'goals', 'cards').
        subkey: Nested key (e.g., 'total', 'yellow').
        default: Default value if not found.

    Returns:
        The extracted value or the default.
    """
    value = stat.get(key, {})
    if value is None:
        return default
    result = value.get(subkey, default)
    return result if result is not None else default


def upsert_player_stats(cursor, player_db_id: int, team_db_id: int,
                        stat: dict, league_id: int, season: int) -> None:
    """Insert or update player statistics.

    Args:
        cursor: Database cursor.
        player_db_id: Internal database ID of the player.
        team_db_id: Internal database ID of the team.
        stat: Statistics dict from the API.
        league_id: API league ID.
        season: Season year.
    """
    games = stat.get("games", {}) or {}
    appearances = games.get("appearences", 0) or 0  # API typo: 'appearences'
    minutes = games.get("minutes", 0) or 0
    position = games.get("position", "")

    # Update player position
    if position:
        cursor.execute(
            "UPDATE players SET position = %s WHERE id = %s",
            (position, player_db_id),
        )

    goals_total = extract_stat(stat, "goals", "total")
    goals_assists = extract_stat(stat, "goals", "assists")
    goals_saves = extract_stat(stat, "goals", "saves")

    shots_total = extract_stat(stat, "shots", "total")
    shots_on = extract_stat(stat, "shots", "on")

    passes_total = extract_stat(stat, "passes", "total")
    passes_key = extract_stat(stat, "passes", "key")
    passes_accuracy = stat.get("passes", {})
    if passes_accuracy:
        passes_accuracy = passes_accuracy.get("accuracy", "0")
    else:
        passes_accuracy = "0"

    tackles_total = extract_stat(stat, "tackles", "total")
    duels_total = extract_stat(stat, "duels", "total")
    duels_won = extract_stat(stat, "duels", "won")

    dribbles_attempts = extract_stat(stat, "dribbles", "attempts")
    dribbles_success = extract_stat(stat, "dribbles", "success")

    fouls_committed = extract_stat(stat, "fouls", "committed")
    fouls_drawn = extract_stat(stat, "fouls", "drawn")

    cards_yellow = extract_stat(stat, "cards", "yellow")
    cards_red = extract_stat(stat, "cards", "red")
    cards_yellowred = extract_stat(stat, "cards", "yellowred")

    penalty_scored = extract_stat(stat, "penalty", "scored")
    penalty_missed = extract_stat(stat, "penalty", "missed")
    penalty_won = extract_stat(stat, "penalty", "won")
    penalty_committed = extract_stat(stat, "penalty", "commited")  # API typo

    cursor.execute(
        """
        INSERT INTO player_stats (
            player_id, team_id, league_id, season,
            appearances, minutes_played,
            goals_total, goals_assists, goals_saves,
            shots_total, shots_on_target,
            passes_total, passes_key, passes_accuracy,
            tackles_total, duels_total, duels_won,
            dribbles_attempts, dribbles_success,
            fouls_committed, fouls_drawn,
            cards_yellow, cards_red, cards_yellowred,
            penalty_scored, penalty_missed, penalty_won, penalty_committed
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (player_id, league_id, season) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            appearances = EXCLUDED.appearances,
            minutes_played = EXCLUDED.minutes_played,
            goals_total = EXCLUDED.goals_total,
            goals_assists = EXCLUDED.goals_assists,
            goals_saves = EXCLUDED.goals_saves,
            shots_total = EXCLUDED.shots_total,
            shots_on_target = EXCLUDED.shots_on_target,
            passes_total = EXCLUDED.passes_total,
            passes_key = EXCLUDED.passes_key,
            passes_accuracy = EXCLUDED.passes_accuracy,
            tackles_total = EXCLUDED.tackles_total,
            duels_total = EXCLUDED.duels_total,
            duels_won = EXCLUDED.duels_won,
            dribbles_attempts = EXCLUDED.dribbles_attempts,
            dribbles_success = EXCLUDED.dribbles_success,
            fouls_committed = EXCLUDED.fouls_committed,
            fouls_drawn = EXCLUDED.fouls_drawn,
            cards_yellow = EXCLUDED.cards_yellow,
            cards_red = EXCLUDED.cards_red,
            cards_yellowred = EXCLUDED.cards_yellowred,
            penalty_scored = EXCLUDED.penalty_scored,
            penalty_missed = EXCLUDED.penalty_missed,
            penalty_won = EXCLUDED.penalty_won,
            penalty_committed = EXCLUDED.penalty_committed,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            player_db_id, team_db_id, league_id, season,
            appearances, minutes,
            goals_total, goals_assists, goals_saves,
            shots_total, shots_on,
            passes_total, passes_key, str(passes_accuracy),
            tackles_total, duels_total, duels_won,
            dribbles_attempts, dribbles_success,
            fouls_committed, fouls_drawn,
            cards_yellow, cards_red, cards_yellowred,
            penalty_scored, penalty_missed, penalty_won, penalty_committed,
        ),
    )


def run_etl() -> None:
    """Execute the full ETL pipeline.

    Steps:
        1. Test database connection and initialize schema.
        2. For each configured league:
            a. Fetch teams.
            b. For each team, fetch all players with stats.
            c. Upsert teams, players, and stats into the database.
        3. Print summary of imported data.
    """
    print("=" * 60)
    print("Previsoes League - Pipeline ETL")
    print("=" * 60)

    # Step 1: Database setup
    if not test_connection():
        print("[ETL] ERROR: Cannot connect to database. Check .env settings.")
        sys.exit(1)

    init_database()

    # Step 2: Initialize API client
    try:
        client = APIFootballClient()
    except ValueError as e:
        print(f"[ETL] ERROR: {e}")
        sys.exit(1)

    # Check API status
    try:
        status = client.check_status()
        account = status.get("account", {})
        subscription = status.get("subscription", {})
        requests_info = status.get("requests", {})
        print(f"[ETL] Account: {account.get('email', 'N/A')}")
        print(f"[ETL] Plan: {subscription.get('plan', 'N/A')}")
        print(f"[ETL] Requests today: {requests_info.get('current', '?')}"
              f"/{requests_info.get('limit_day', '?')}")
    except Exception as e:
        print(f"[ETL] Warning: Could not check API status: {e}")

    total_teams = 0
    total_players = 0
    total_stats = 0

    for league_id in LEAGUE_IDS:
        print(f"\n{'='*40}")
        print(f"[ETL] Processing league ID: {league_id} | Season: {SEASON}")
        print(f"{'='*40}")

        # Step 3: Fetch teams
        try:
            teams_data = client.get_teams(league_id, SEASON)
        except Exception as e:
            print(f"[ETL] ERROR fetching teams for league {league_id}: {e}")
            continue

        teams_to_process = teams_data[:MAX_TEAMS]
        print(f"[ETL] Found {len(teams_data)} teams, "
              f"processing {len(teams_to_process)}")

        for idx, team_entry in enumerate(teams_to_process):
            team_info = team_entry.get("team", {})
            team_name = team_info.get("name", "Unknown")
            team_api_id = team_info.get("id")

            print(f"\n[ETL] [{idx+1}/{len(teams_to_process)}] "
                  f"Processing team: {team_name}")

            with get_cursor() as cursor:
                # Determine league name from team_entry if available
                league_name = LEAGUE_NAMES.get(league_id, f"League {league_id}")
                venue = team_entry.get("venue", {})
                country = team_info.get("country", venue.get("city", ""))

                team_data_for_db = {
                    "id": team_api_id,
                    "name": team_name,
                    "logo": team_info.get("logo", ""),
                    "country": country,
                }

                team_db_id = upsert_team(
                    cursor,
                    {"team": team_data_for_db},
                    league_id,
                    league_name,
                    SEASON,
                )
                total_teams += 1

            # Step 4: Fetch players for this team
            try:
                players_data = client.get_all_players_for_team(
                    team_api_id, SEASON
                )
            except Exception as e:
                print(f"[ETL] ERROR fetching players for {team_name}: {e}")
                continue

            print(f"[ETL] Found {len(players_data)} players for {team_name}")

            with get_cursor() as cursor:
                for player_entry in players_data:
                    player_info = player_entry.get("player", {})
                    statistics = player_entry.get("statistics", [])

                    if not player_info.get("id"):
                        continue

                    player_db_id = upsert_player(
                        cursor, player_info, team_db_id
                    )
                    total_players += 1

                    # Process each statistics entry (one per league/team)
                    for stat in statistics:
                        stat_league = stat.get("league", {}) or {}
                        stat_league_id = stat_league.get("id", league_id)

                        # Only process stats for the target league
                        if stat_league_id != league_id:
                            continue

                        upsert_player_stats(
                            cursor,
                            player_db_id,
                            team_db_id,
                            stat,
                            league_id,
                            SEASON,
                        )
                        total_stats += 1

    # Summary
    print(f"\n{'='*60}")
    print("[ETL] Pipeline completed successfully!")
    print(f"[ETL] Teams imported: {total_teams}")
    print(f"[ETL] Players imported: {total_players}")
    print(f"[ETL] Stats records: {total_stats}")
    print(f"{'='*60}")


if __name__ == "__main__":
    run_etl()
