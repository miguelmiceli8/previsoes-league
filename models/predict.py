"""Prediction models for football player events.

Calculates probabilities based on historical per-game averages.

Probability formula:
    P(event per game) = total_events / appearances

This simple model assumes that a player's future performance follows
their historical average. More sophisticated models could incorporate:
- Home/away splits
- Form (recent matches)
- Opposition strength
- Minutes played normalization
"""

import sys
import os
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_cursor


def _safe_divide(numerator: float, denominator: float) -> float:
    """Safely divide two numbers, returning 0.0 if denominator is zero."""
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def get_top_scorers(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 5,
    min_appearances: int = 5,
) -> list[dict]:
    """Get players with highest goal probability per game.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Number of top players to return.
        min_appearances: Minimum appearances to be considered.

    Returns:
        List of dicts with player info and goal probability.
    """
    with get_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                p.name,
                p.photo,
                p.position,
                t.name AS team_name,
                t.logo AS team_logo,
                ps.appearances,
                ps.minutes_played,
                ps.goals_total,
                ps.goals_assists,
                ps.shots_total,
                ps.shots_on_target,
                ps.penalty_scored,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(ps.goals_total::NUMERIC / ps.appearances, 4)
                    ELSE 0
                END AS goals_per_game,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(
                        (1 - POWER(
                            1 - (ps.goals_total::NUMERIC / ps.appearances),
                            1
                        )) * 100, 2
                    )
                    ELSE 0
                END AS goal_probability_pct
            FROM player_stats ps
            JOIN players p ON p.id = ps.player_id
            JOIN teams t ON t.id = ps.team_id
            WHERE ps.league_id = %s
              AND ps.season = %s
              AND ps.appearances >= %s
              AND ps.goals_total > 0
            ORDER BY goals_per_game DESC
            LIMIT %s
            """,
            (league_id, season, min_appearances, limit),
        )
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "goals": row["goals_total"],
            "assists": row["goals_assists"],
            "shots": row["shots_total"],
            "shots_on_target": row["shots_on_target"],
            "penalties_scored": row["penalty_scored"],
            "goals_per_game": float(row["goals_per_game"]),
            "goal_probability_pct": float(row["goal_probability_pct"]),
        })
    return results


def get_top_cards(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 5,
    min_appearances: int = 5,
    card_type: str = "yellow",
) -> list[dict]:
    """Get players with highest card probability per game.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Number of top players to return.
        min_appearances: Minimum appearances.
        card_type: 'yellow', 'red', or 'any'.

    Returns:
        List of dicts with player info and card probability.
    """
    if card_type == "yellow":
        card_col = "ps.cards_yellow"
    elif card_type == "red":
        card_col = "ps.cards_red"
    else:
        card_col = "(ps.cards_yellow + ps.cards_red + ps.cards_yellowred)"

    query = f"""
        SELECT
            p.name,
            p.photo,
            p.position,
            t.name AS team_name,
            t.logo AS team_logo,
            ps.appearances,
            ps.minutes_played,
            ps.cards_yellow,
            ps.cards_red,
            ps.cards_yellowred,
            ps.fouls_committed,
            ps.tackles_total,
            {card_col} AS card_count,
            CASE
                WHEN ps.appearances > 0
                THEN ROUND({card_col}::NUMERIC / ps.appearances, 4)
                ELSE 0
            END AS cards_per_game,
            CASE
                WHEN ps.appearances > 0
                THEN ROUND(
                    (1 - POWER(
                        GREATEST(1 - ({card_col}::NUMERIC / ps.appearances), 0),
                        1
                    )) * 100, 2
                )
                ELSE 0
            END AS card_probability_pct
        FROM player_stats ps
        JOIN players p ON p.id = ps.player_id
        JOIN teams t ON t.id = ps.team_id
        WHERE ps.league_id = %s
          AND ps.season = %s
          AND ps.appearances >= %s
          AND {card_col} > 0
        ORDER BY cards_per_game DESC
        LIMIT %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, (league_id, season, min_appearances, limit))
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "yellow_cards": row["cards_yellow"],
            "red_cards": row["cards_red"],
            "second_yellow_cards": row["cards_yellowred"],
            "fouls_committed": row["fouls_committed"],
            "tackles": row["tackles_total"],
            "card_count": row["card_count"],
            "cards_per_game": float(row["cards_per_game"]),
            "card_probability_pct": float(row["card_probability_pct"]),
        })
    return results


def get_top_assists(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 5,
    min_appearances: int = 5,
) -> list[dict]:
    """Get players with highest assist probability per game.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Number of top players to return.
        min_appearances: Minimum appearances.

    Returns:
        List of dicts with player info and assist probability.
    """
    with get_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                p.name,
                p.photo,
                p.position,
                t.name AS team_name,
                t.logo AS team_logo,
                ps.appearances,
                ps.minutes_played,
                ps.goals_assists,
                ps.passes_total,
                ps.passes_key,
                ps.passes_accuracy,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(ps.goals_assists::NUMERIC / ps.appearances, 4)
                    ELSE 0
                END AS assists_per_game,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(
                        (1 - POWER(
                            1 - (ps.goals_assists::NUMERIC / ps.appearances),
                            1
                        )) * 100, 2
                    )
                    ELSE 0
                END AS assist_probability_pct
            FROM player_stats ps
            JOIN players p ON p.id = ps.player_id
            JOIN teams t ON t.id = ps.team_id
            WHERE ps.league_id = %s
              AND ps.season = %s
              AND ps.appearances >= %s
              AND ps.goals_assists > 0
            ORDER BY assists_per_game DESC
            LIMIT %s
            """,
            (league_id, season, min_appearances, limit),
        )
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "assists": row["goals_assists"],
            "total_passes": row["passes_total"],
            "key_passes": row["passes_key"],
            "pass_accuracy": row["passes_accuracy"],
            "assists_per_game": float(row["assists_per_game"]),
            "assist_probability_pct": float(row["assist_probability_pct"]),
        })
    return results


def get_top_fouls_committed(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 5,
    min_appearances: int = 5,
) -> list[dict]:
    """Get players who commit the most fouls per game.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Number of top players to return.
        min_appearances: Minimum appearances.

    Returns:
        List of dicts with player info and foul probability.
    """
    with get_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                p.name,
                p.photo,
                p.position,
                t.name AS team_name,
                t.logo AS team_logo,
                ps.appearances,
                ps.minutes_played,
                ps.fouls_committed,
                ps.cards_yellow,
                ps.cards_red,
                ps.tackles_total,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(ps.fouls_committed::NUMERIC / ps.appearances, 4)
                    ELSE 0
                END AS fouls_per_game
            FROM player_stats ps
            JOIN players p ON p.id = ps.player_id
            JOIN teams t ON t.id = ps.team_id
            WHERE ps.league_id = %s
              AND ps.season = %s
              AND ps.appearances >= %s
              AND ps.fouls_committed > 0
            ORDER BY fouls_per_game DESC
            LIMIT %s
            """,
            (league_id, season, min_appearances, limit),
        )
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "fouls_committed": row["fouls_committed"],
            "yellow_cards": row["cards_yellow"],
            "red_cards": row["cards_red"],
            "tackles": row["tackles_total"],
            "fouls_per_game": float(row["fouls_per_game"]),
        })
    return results


def get_top_fouls_drawn(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 5,
    min_appearances: int = 5,
) -> list[dict]:
    """Get players who draw the most fouls per game.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Number of top players to return.
        min_appearances: Minimum appearances.

    Returns:
        List of dicts with player info and fouls drawn data.
    """
    with get_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                p.name,
                p.photo,
                p.position,
                t.name AS team_name,
                t.logo AS team_logo,
                ps.appearances,
                ps.minutes_played,
                ps.fouls_drawn,
                ps.dribbles_attempts,
                ps.dribbles_success,
                ps.penalty_won,
                CASE
                    WHEN ps.appearances > 0
                    THEN ROUND(ps.fouls_drawn::NUMERIC / ps.appearances, 4)
                    ELSE 0
                END AS fouls_drawn_per_game
            FROM player_stats ps
            JOIN players p ON p.id = ps.player_id
            JOIN teams t ON t.id = ps.team_id
            WHERE ps.league_id = %s
              AND ps.season = %s
              AND ps.appearances >= %s
              AND ps.fouls_drawn > 0
            ORDER BY fouls_drawn_per_game DESC
            LIMIT %s
            """,
            (league_id, season, min_appearances, limit),
        )
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "fouls_drawn": row["fouls_drawn"],
            "dribble_attempts": row["dribbles_attempts"],
            "dribble_success": row["dribbles_success"],
            "penalties_won": row["penalty_won"],
            "fouls_drawn_per_game": float(row["fouls_drawn_per_game"]),
        })
    return results


def get_all_players(
    league_id: int = 39,
    season: int = 2024,
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None,
) -> list[dict]:
    """Get all players with their statistics.

    Args:
        league_id: API league ID.
        season: Season year.
        limit: Maximum number of players to return.
        offset: Offset for pagination.
        search: Optional player name search filter.

    Returns:
        List of dicts with player info and all stats.
    """
    search_clause = ""
    params: list = [league_id, season]

    if search:
        search_clause = "AND LOWER(p.name) LIKE LOWER(%s)"
        params.append(f"%{search}%")

    params.extend([limit, offset])

    query = f"""
        SELECT
            p.name,
            p.photo,
            p.position,
            p.nationality,
            p.age,
            t.name AS team_name,
            t.logo AS team_logo,
            ps.appearances,
            ps.minutes_played,
            ps.goals_total,
            ps.goals_assists,
            ps.cards_yellow,
            ps.cards_red,
            ps.fouls_committed,
            ps.fouls_drawn,
            ps.shots_total,
            ps.shots_on_target,
            ps.passes_key,
            ps.tackles_total,
            ps.dribbles_success
        FROM player_stats ps
        JOIN players p ON p.id = ps.player_id
        JOIN teams t ON t.id = ps.team_id
        WHERE ps.league_id = %s
          AND ps.season = %s
          {search_clause}
        ORDER BY ps.appearances DESC, p.name ASC
        LIMIT %s OFFSET %s
    """

    with get_cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "player": row["name"],
            "photo": row["photo"],
            "position": row["position"],
            "nationality": row["nationality"],
            "age": row["age"],
            "team": row["team_name"],
            "team_logo": row["team_logo"],
            "appearances": row["appearances"],
            "minutes_played": row["minutes_played"],
            "goals": row["goals_total"],
            "assists": row["goals_assists"],
            "yellow_cards": row["cards_yellow"],
            "red_cards": row["cards_red"],
            "fouls_committed": row["fouls_committed"],
            "fouls_drawn": row["fouls_drawn"],
            "shots": row["shots_total"],
            "shots_on_target": row["shots_on_target"],
            "key_passes": row["passes_key"],
            "tackles": row["tackles_total"],
            "dribbles": row["dribbles_success"],
        })
    return results


if __name__ == "__main__":
    print("Top Scorers:")
    for p in get_top_scorers():
        print(f"  {p['player']} ({p['team']}): "
              f"{p['goals']} goals in {p['appearances']} apps "
              f"= {p['goal_probability_pct']}%")

    print("\nTop Yellow Cards:")
    for p in get_top_cards(card_type="yellow"):
        print(f"  {p['player']} ({p['team']}): "
              f"{p['yellow_cards']} yellows in {p['appearances']} apps "
              f"= {p['card_probability_pct']}%")

    print("\nTop Assists:")
    for p in get_top_assists():
        print(f"  {p['player']} ({p['team']}): "
              f"{p['assists']} assists in {p['appearances']} apps "
              f"= {p['assist_probability_pct']}%")

    print("\nTop Fouls Committed:")
    for p in get_top_fouls_committed():
        print(f"  {p['player']} ({p['team']}): "
              f"{p['fouls_committed']} fouls in {p['appearances']} apps "
              f"= {p['fouls_per_game']}/game")

    print("\nTop Fouls Drawn:")
    for p in get_top_fouls_drawn():
        print(f"  {p['player']} ({p['team']}): "
              f"{p['fouls_drawn']} fouls drawn in {p['appearances']} apps "
              f"= {p['fouls_drawn_per_game']}/game")
