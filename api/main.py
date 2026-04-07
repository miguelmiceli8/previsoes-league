"""API FastAPI para Previsoes League.

Endpoints:
    GET /                    - Health check
    GET /players             - Listar jogadores com estatisticas
    GET /players/top-scorers - Artilheiros por probabilidade de gol
    GET /players/top-cards   - Jogadores por probabilidade de cartao
    GET /players/top-assists - Jogadores por probabilidade de assistencia
    GET /players/top-fouls-committed - Jogadores que mais cometem faltas
    GET /players/top-fouls-drawn    - Jogadores que mais sofrem faltas

Uso:
    uvicorn api.main:app --reload --port 8000
"""

import sys
import os
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.predict import (
    get_all_players,
    get_top_assists,
    get_top_cards,
    get_top_fouls_committed,
    get_top_fouls_drawn,
    get_top_scorers,
)

app = FastAPI(
    title="Previsoes League",
    description=(
        "API de analise preditiva de jogadores da Premier League 2024. "
        "Calcula probabilidades por jogo para gols, cartoes, "
        "assistencias e faltas."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "Previsoes League"}


@app.get("/players")
def list_players(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(50, ge=1, le=500, description="Max jogadores retornados"),
    offset: int = Query(0, ge=0, description="Offset para paginacao"),
    search: Optional[str] = Query(None, description="Buscar jogador por nome"),
):
    """Listar todos os jogadores com estatisticas.

    Suporta paginacao e busca por nome.
    """
    players = get_all_players(
        league_id=league_id,
        season=season,
        limit=limit,
        offset=offset,
        search=search,
    )
    return {
        "category": "all_players",
        "league_id": league_id,
        "season": season,
        "count": len(players),
        "data": players,
    }


@app.get("/players/top-scorers")
def top_scorers(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(5, ge=1, le=50, description="Numero de jogadores no ranking"),
    min_appearances: int = Query(
        5, ge=1, description="Minimo de jogos para filtro"
    ),
):
    """Artilheiros ranqueados por probabilidade de gol por jogo.

    Retorna jogadores com:
    - Total de gols e assistencias
    - Finalizacoes e finalizacoes no gol
    - Gols por jogo
    - Probabilidade de gol
    """
    scorers = get_top_scorers(
        league_id=league_id,
        season=season,
        limit=limit,
        min_appearances=min_appearances,
    )
    return {
        "category": "top_scorers",
        "league_id": league_id,
        "season": season,
        "min_appearances": min_appearances,
        "count": len(scorers),
        "data": scorers,
    }


@app.get("/players/top-cards")
def top_cards(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(5, ge=1, le=50, description="Numero de jogadores no ranking"),
    min_appearances: int = Query(
        5, ge=1, description="Minimo de jogos para filtro"
    ),
    card_type: str = Query(
        "yellow",
        description="Tipo de cartao: 'yellow', 'red', ou 'any'",
    ),
):
    """Jogadores com maior probabilidade de cartao por jogo.

    Retorna jogadores com:
    - Cartoes amarelos, vermelhos e segundo amarelo
    - Faltas cometidas e desarmes
    - Cartoes por jogo
    - Probabilidade de cartao
    """
    cards = get_top_cards(
        league_id=league_id,
        season=season,
        limit=limit,
        min_appearances=min_appearances,
        card_type=card_type,
    )
    return {
        "category": f"top_cards_{card_type}",
        "league_id": league_id,
        "season": season,
        "min_appearances": min_appearances,
        "count": len(cards),
        "data": cards,
    }


@app.get("/players/top-assists")
def top_assists(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(5, ge=1, le=50, description="Numero de jogadores no ranking"),
    min_appearances: int = Query(
        5, ge=1, description="Minimo de jogos para filtro"
    ),
):
    """Maiores assistentes ranqueados por probabilidade de assistencia por jogo.

    Retorna jogadores com:
    - Total de assistencias
    - Estatisticas de passes (total, chave, precisao)
    - Assistencias por jogo
    - Probabilidade de assistencia
    """
    assists = get_top_assists(
        league_id=league_id,
        season=season,
        limit=limit,
        min_appearances=min_appearances,
    )
    return {
        "category": "top_assists",
        "league_id": league_id,
        "season": season,
        "min_appearances": min_appearances,
        "count": len(assists),
        "data": assists,
    }


@app.get("/players/top-fouls-committed")
def top_fouls_committed(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(5, ge=1, le=50, description="Numero de jogadores no ranking"),
    min_appearances: int = Query(
        5, ge=1, description="Minimo de jogos para filtro"
    ),
):
    """Jogadores que mais cometem faltas por jogo.

    Retorna jogadores com:
    - Total de faltas cometidas
    - Historico de cartoes
    - Desarmes
    - Faltas por jogo
    """
    fouls = get_top_fouls_committed(
        league_id=league_id,
        season=season,
        limit=limit,
        min_appearances=min_appearances,
    )
    return {
        "category": "top_fouls_committed",
        "league_id": league_id,
        "season": season,
        "min_appearances": min_appearances,
        "count": len(fouls),
        "data": fouls,
    }


@app.get("/players/top-fouls-drawn")
def top_fouls_drawn(
    league_id: int = Query(
        39,
        description="ID da liga (39=Premier League)",
    ),
    season: int = Query(2024, description="Ano da temporada"),
    limit: int = Query(5, ge=1, le=50, description="Numero de jogadores no ranking"),
    min_appearances: int = Query(
        5, ge=1, description="Minimo de jogos para filtro"
    ),
):
    """Jogadores que mais sofrem faltas por jogo.

    Retorna jogadores com:
    - Total de faltas sofridas
    - Estatisticas de dribles
    - Penaltis ganhos
    - Faltas sofridas por jogo
    """
    fouls = get_top_fouls_drawn(
        league_id=league_id,
        season=season,
        limit=limit,
        min_appearances=min_appearances,
    )
    return {
        "category": "top_fouls_drawn",
        "league_id": league_id,
        "season": season,
        "min_appearances": min_appearances,
        "count": len(fouls),
        "data": fouls,
    }
