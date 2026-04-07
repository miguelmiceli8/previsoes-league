"""Dashboard Streamlit para Previsoes League.

Exibe rankings e probabilidades de jogadores da Premier League 2024:
- Gols
- Cartoes (amarelo / vermelho)
- Assistencias
- Faltas cometidas
- Faltas sofridas

Uso:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.predict import (
    get_all_players,
    get_top_scorers,
    get_top_cards,
    get_top_assists,
    get_top_fouls_committed,
    get_top_fouls_drawn,
)

# Configuracao fixa: Premier League 2024
LEAGUE_ID = 39
SEASON = 2024

st.set_page_config(
    page_title="Previsoes League",
    page_icon="⚽",
    layout="wide",
)

# CSS customizado com tema Premier League
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(180deg, #1a0a2e 0%, #16213e 50%, #0f3460 100%);
    }
    .premier-header {
        background: linear-gradient(135deg, #3d195b 0%, #6c2b91 100%);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        text-align: center;
        border: 2px solid #e90052;
    }
    .premier-header h1 {
        color: #ffffff;
        font-size: 2.5rem;
        margin: 0;
        font-weight: 800;
    }
    .premier-header p {
        color: #e0d0f0;
        font-size: 1.1rem;
        margin-top: 0.5rem;
    }
    .premier-badge {
        color: #00ff87;
        font-weight: 700;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #3d195b 0%, #2d1050 100%);
    }
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3,
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown label {
        color: #ffffff !important;
    }
    .footer {
        text-align: center;
        color: #a0a0c0;
        padding: 1.5rem;
        margin-top: 2rem;
        border-top: 1px solid #3d195b;
        font-size: 0.9rem;
    }
    .footer .devs {
        color: #00ff87;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def fetch_data(fn, **kwargs) -> list:
    """Chama funcao do modelo com tratamento de erro."""
    try:
        return fn(**kwargs)
    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
        return []


def render_player_table(data: list, key_columns: dict) -> None:
    """Renderizar tabela estilizada de ranking de jogadores."""
    if not data:
        st.warning("Nenhum dado disponivel. Verifique a conexao com o banco.")
        return

    rows = []
    for i, player in enumerate(data, 1):
        row = {"#": i, "Jogador": player.get("player", "")}
        row["Time"] = player.get("team", "")
        row["Posicao"] = player.get("position", "")
        row["Jogos"] = player.get("appearances", 0)

        for display_name, data_key in key_columns.items():
            value = player.get(data_key, 0)
            if isinstance(value, float) and "pct" in data_key:
                row[display_name] = f"{value:.1f}%"
            elif isinstance(value, float):
                row[display_name] = f"{value:.2f}"
            else:
                row[display_name] = value

        rows.append(row)

    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "#": st.column_config.NumberColumn(width="small"),
            "Jogador": st.column_config.TextColumn(width="medium"),
            "Time": st.column_config.TextColumn(width="medium"),
        },
    )


def main():
    """Aplicacao principal Streamlit."""
    st.markdown("""
    <div class="premier-header">
        <h1>⚽ Previsoes League</h1>
        <p>Analise preditiva de jogadores da
        <span class="premier-badge">Premier League 2024</span>
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.sidebar.markdown("## ⚙️ Configuracoes")
    st.sidebar.markdown(
        "🏴󠁧󠁢󠁥󠁮󠁧󠁿 **Premier League** — Temporada **2024**"
    )
    st.sidebar.divider()

    top_n = st.sidebar.slider("Top N Jogadores", min_value=3, max_value=20, value=5)
    min_apps = st.sidebar.slider("Min. de Jogos", min_value=1, max_value=30, value=5)

    tab_goals, tab_cards, tab_assists, tab_fouls_c, tab_fouls_d, tab_all = (
        st.tabs([
            "🥅 Gols",
            "🟨 Cartoes",
            "🅰️ Assistencias",
            "💪 Faltas Cometidas",
            "🤕 Faltas Sofridas",
            "📋 Todos os Jogadores",
        ])
    )

    with tab_goals:
        st.header("🥅 Maiores Artilheiros")
        st.caption("Probabilidade = gols / jogos x 100")
        data = fetch_data(
            get_top_scorers,
            league_id=LEAGUE_ID, season=SEASON,
            limit=top_n, min_appearances=min_apps,
        )
        render_player_table(data, {
            "Gols": "goals",
            "Assist.": "assists",
            "Finalizacoes": "shots",
            "No Gol": "shots_on_target",
            "Gols/Jogo": "goals_per_game",
            "Probabilidade": "goal_probability_pct",
        })
        if data:
            st.subheader("📊 Distribuicao de Gols por Jogo")
            chart_df = pd.DataFrame(data)[["player", "goals_per_game"]].rename(
                columns={"player": "Jogador", "goals_per_game": "Gols/Jogo"}
            )
            st.bar_chart(chart_df.set_index("Jogador"))

    with tab_cards:
        st.header("🟨 Maiores Recebedores de Cartao")
        card_type = st.radio(
            "Tipo de Cartao",
            ["yellow", "red", "any"],
            format_func=lambda x: {
                "yellow": "🟨 Cartoes Amarelos",
                "red": "🟥 Cartoes Vermelhos",
                "any": "Todos os Cartoes",
            }[x],
            horizontal=True,
        )
        st.caption("Probabilidade = cartoes / jogos x 100")
        data = fetch_data(
            get_top_cards,
            league_id=LEAGUE_ID, season=SEASON,
            limit=top_n, min_appearances=min_apps, card_type=card_type,
        )
        render_player_table(data, {
            "Amarelos": "yellow_cards",
            "Vermelhos": "red_cards",
            "2o Amarelo": "second_yellow_cards",
            "Faltas": "fouls_committed",
            "Cartoes/Jogo": "cards_per_game",
            "Probabilidade": "card_probability_pct",
        })
        if data:
            st.subheader("📊 Distribuicao de Cartoes por Jogo")
            chart_df = pd.DataFrame(data)[["player", "cards_per_game"]].rename(
                columns={"player": "Jogador", "cards_per_game": "Cartoes/Jogo"}
            )
            st.bar_chart(chart_df.set_index("Jogador"))

    with tab_assists:
        st.header("🅰️ Maiores Assistentes")
        st.caption("Probabilidade = assistencias / jogos x 100")
        data = fetch_data(
            get_top_assists,
            league_id=LEAGUE_ID, season=SEASON,
            limit=top_n, min_appearances=min_apps,
        )
        render_player_table(data, {
            "Assist.": "assists",
            "Passes Chave": "key_passes",
            "Total Passes": "total_passes",
            "Prec. Passe": "pass_accuracy",
            "Assist./Jogo": "assists_per_game",
            "Probabilidade": "assist_probability_pct",
        })
        if data:
            st.subheader("📊 Distribuicao de Assistencias por Jogo")
            chart_df = pd.DataFrame(data)[["player", "assists_per_game"]].rename(
                columns={"player": "Jogador", "assists_per_game": "Assist./Jogo"}
            )
            st.bar_chart(chart_df.set_index("Jogador"))

    with tab_fouls_c:
        st.header("💪 Maiores Faltosos")
        st.caption("Jogadores ranqueados por faltas cometidas por jogo.")
        data = fetch_data(
            get_top_fouls_committed,
            league_id=LEAGUE_ID, season=SEASON,
            limit=top_n, min_appearances=min_apps,
        )
        render_player_table(data, {
            "Faltas": "fouls_committed",
            "Amarelos": "yellow_cards",
            "Vermelhos": "red_cards",
            "Desarmes": "tackles",
            "Faltas/Jogo": "fouls_per_game",
        })
        if data:
            st.subheader("📊 Distribuicao de Faltas por Jogo")
            chart_df = pd.DataFrame(data)[["player", "fouls_per_game"]].rename(
                columns={"player": "Jogador", "fouls_per_game": "Faltas/Jogo"}
            )
            st.bar_chart(chart_df.set_index("Jogador"))

    with tab_fouls_d:
        st.header("🤕 Mais Sofrem Faltas")
        st.caption("Jogadores ranqueados por faltas sofridas por jogo.")
        data = fetch_data(
            get_top_fouls_drawn,
            league_id=LEAGUE_ID, season=SEASON,
            limit=top_n, min_appearances=min_apps,
        )
        render_player_table(data, {
            "Faltas Sofridas": "fouls_drawn",
            "Dribles Tent.": "dribble_attempts",
            "Dribles Certos": "dribble_success",
            "Penaltis Ganhos": "penalties_won",
            "Faltas Sofr./Jogo": "fouls_drawn_per_game",
        })
        if data:
            st.subheader("📊 Distribuicao de Faltas Sofridas por Jogo")
            chart_df = pd.DataFrame(data)[["player", "fouls_drawn_per_game"]].rename(
                columns={"player": "Jogador", "fouls_drawn_per_game": "Faltas Sofr./Jogo"}
            )
            st.bar_chart(chart_df.set_index("Jogador"))

    with tab_all:
        st.header("📋 Todos os Jogadores")
        search = st.text_input("🔍 Buscar jogador por nome", "")
        data = fetch_data(
            get_all_players,
            league_id=LEAGUE_ID, season=SEASON,
            limit=100, offset=0,
            search=search if search else None,
        )
        if data:
            df = pd.DataFrame(data)
            col_rename = {
                "player": "jogador", "team": "time", "position": "posicao",
                "nationality": "nacionalidade", "age": "idade",
                "appearances": "jogos", "goals": "gols",
                "assists": "assistencias", "yellow_cards": "cartoes_amarelos",
                "red_cards": "cartoes_vermelhos",
                "fouls_committed": "faltas_cometidas",
                "fouls_drawn": "faltas_sofridas", "shots": "finalizacoes",
                "key_passes": "passes_chave", "tackles": "desarmes",
            }
            display_cols = [
                "player", "team", "position", "nationality", "age",
                "appearances", "goals", "assists", "yellow_cards",
                "red_cards", "fouls_committed", "fouls_drawn",
                "shots", "key_passes", "tackles",
            ]
            existing_cols = [c for c in display_cols if c in df.columns]
            display_df = df[existing_cols].rename(
                columns={k: v for k, v in col_rename.items() if k in existing_cols}
            )
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("Nenhum dado de jogador disponivel.")

    st.divider()
    st.markdown(
        "<div class='footer'>"
        "Previsoes League — Analise Preditiva da Premier League 2024<br>"
        "Desenvolvido por <span class='devs'>Miguel Miceli</span> e "
        "<span class='devs'>Allan Patrick Fantoni</span>"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()