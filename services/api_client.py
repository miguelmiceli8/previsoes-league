"""API-Football client for fetching football data.

Uses the free tier of API-Football (https://www.api-football.com/).
Free tier allows 100 requests/day.

Docs: https://www.api-football.com/documentation-v3
"""

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://v3.football.api-sports.io"

# Rate limiting: free tier = 10 requests per minute
REQUEST_DELAY = 6.5  # seconds between requests to stay within limits


class APIFootballClient:
    """Client to interact with the API-Football v3 API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "API_KEY not found. Set it in .env or pass it directly."
            )
        self.headers = {
            "x-apisports-key": self.api_key,
        }
        self._last_request_time = 0.0

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            wait = REQUEST_DELAY - elapsed
            print(f"[API] Rate limiting: waiting {wait:.1f}s...")
            time.sleep(wait)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """Make a GET request to the API.

        Args:
            endpoint: API endpoint path (e.g., '/players').
            params: Query parameters.

        Returns:
            JSON response as dict.

        Raises:
            requests.HTTPError: If the request fails.
        """
        self._rate_limit()
        url = f"{BASE_URL}{endpoint}"
        print(f"[API] GET {url} params={params}")

        response = requests.get(url, headers=self.headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        if data.get("errors"):
            errors = data["errors"]
            if isinstance(errors, dict):
                error_msg = "; ".join(f"{k}: {v}" for k, v in errors.items())
            elif isinstance(errors, list):
                error_msg = "; ".join(str(e) for e in errors)
            else:
                error_msg = str(errors)
            raise ValueError(f"API Error: {error_msg}")

        remaining = response.headers.get("x-ratelimit-requests-remaining", "?")
        print(f"[API] Requests remaining today: {remaining}")

        return data

    def get_leagues(self, country: Optional[str] = None) -> list:
        """Fetch available leagues, optionally filtered by country.

        Args:
            country: Country name filter (e.g., 'England').

        Returns:
            List of league data dicts.
        """
        params = {}
        if country:
            params["country"] = country
        data = self._get("/leagues", params)
        return data.get("response", [])

    def get_teams(self, league_id: int, season: int) -> list:
        """Fetch teams for a specific league and season.

        Args:
            league_id: The API league ID.
            season: Season year (e.g., 2024).

        Returns:
            List of team data dicts.
        """
        data = self._get("/teams", {"league": league_id, "season": season})
        return data.get("response", [])

    def get_players(
        self, team_id: int, season: int, page: int = 1
    ) -> dict:
        """Fetch player statistics for a team in a season.

        The API returns paginated results (20 players per page).

        Args:
            team_id: The API team ID.
            season: Season year.
            page: Page number.

        Returns:
            Full response dict including paging info.
        """
        data = self._get(
            "/players",
            {"team": team_id, "season": season, "page": page},
        )
        return data

    def get_all_players_for_team(
        self, team_id: int, season: int, max_pages: int = 3
    ) -> list:
        """Fetch all players for a team across all pages.

        Args:
            team_id: The API team ID.
            season: Season year.
            max_pages: Maximum pages to fetch (free tier limit is 3).

        Returns:
            List of all player data dicts for the team.
        """
        all_players = []
        page = 1

        while True:
            data = self.get_players(team_id, season, page)
            response = data.get("response", [])
            paging = data.get("paging", {})

            all_players.extend(response)

            current = paging.get("current", page)
            total = paging.get("total", page)

            print(f"[API] Team {team_id}: page {current}/{total} "
                  f"({len(response)} players)")

            if current >= total or current >= max_pages:
                if current < total:
                    print(f"[API] Stopped at page {current}/{total} "
                          f"(free tier limit: {max_pages} pages)")
                break
            page += 1

        return all_players

    def get_top_scorers(self, league_id: int, season: int) -> list:
        """Fetch top scorers for a league/season.

        Args:
            league_id: The API league ID.
            season: Season year.

        Returns:
            List of top scorer data dicts.
        """
        data = self._get(
            "/players/topscorers",
            {"league": league_id, "season": season},
        )
        return data.get("response", [])

    def get_top_assists(self, league_id: int, season: int) -> list:
        """Fetch top assist providers for a league/season.

        Args:
            league_id: The API league ID.
            season: Season year.

        Returns:
            List of top assist data dicts.
        """
        data = self._get(
            "/players/topassists",
            {"league": league_id, "season": season},
        )
        return data.get("response", [])

    def get_top_yellow_cards(self, league_id: int, season: int) -> list:
        """Fetch players with most yellow cards for a league/season.

        Args:
            league_id: The API league ID.
            season: Season year.

        Returns:
            List of player data dicts.
        """
        data = self._get(
            "/players/topyellowcards",
            {"league": league_id, "season": season},
        )
        return data.get("response", [])

    def get_top_red_cards(self, league_id: int, season: int) -> list:
        """Fetch players with most red cards for a league/season.

        Args:
            league_id: The API league ID.
            season: Season year.

        Returns:
            List of player data dicts.
        """
        data = self._get(
            "/players/topredcards",
            {"league": league_id, "season": season},
        )
        return data.get("response", [])

    def check_status(self) -> dict:
        """Check API account status and remaining requests.

        Returns:
            Account status information.
        """
        data = self._get("/status")
        return data.get("response", {})
