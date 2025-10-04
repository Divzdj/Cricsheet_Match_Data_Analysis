import os
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns

#  Configuration 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, 'cricket_data.db')
ENGINE = create_engine(f"sqlite:///{DATABASE_PATH}", echo=False)
VISUALS_DIR = os.path.join(BASE_DIR, 'data', 'results', 'visuals')

# Create visuals folder and set plotting style
os.makedirs(VISUALS_DIR, exist_ok=True)
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)

# Data Loading Utility 
def load_data(table_name, limit=None):
    #Loads data from a specified table.
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT * FROM {table_name} {limit_clause}"
    try:
        with ENGINE.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"Could not load data for {table_name}: {e}")
        return pd.DataFrame()

#  Visualization Functions (10 Required Visuals) 

def plot_1_match_volume_over_time():
    """V1: Time Series - Total Matches Played Per Year across all formats."""
    print("Generating V1: Match Volume Over Time...")
    query = """
    SELECT
        CAST(SUBSTR(date, 1, 4) AS INTEGER) AS MatchYear,
        COUNT(match_id) AS MatchesPlayed
    FROM (
        SELECT match_id, date FROM test_matches
        UNION ALL SELECT match_id, date FROM odi_matches
        UNION ALL SELECT match_id, date FROM t20_matches
        UNION ALL SELECT match_id, date FROM ipl_matches
    )
    GROUP BY MatchYear
    ORDER BY MatchYear;
    """
    df_yearly = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.lineplot(x='MatchYear', y='MatchesPlayed', data=df_yearly, marker='o')
    plt.title('V1: Total Matches Played Per Year (All Formats)')
    plt.xlabel('Year')
    plt.ylabel('Matches Played')
    plt.savefig(os.path.join(VISUALS_DIR, 'V1_Match_Volume_Trend.png'))
    plt.close()

def plot_2_run_rate_distribution_by_format():
    """V2: Box Plot - Distribution of Run Rates per Over by Match Format."""
    print("Generating V2: Run Rate Distribution by Format...")
    query = """
    SELECT
        T2.format,
        CAST(SUM(T1.runs_total) * 6.0 AS REAL) / COUNT(T1.delivery_id) AS RunRate
    FROM (
        SELECT match_id, runs_total, delivery_id FROM test_deliveries
        UNION ALL SELECT match_id, runs_total, delivery_id FROM odi_deliveries
        UNION ALL SELECT match_id, runs_total, delivery_id FROM t20_deliveries
        UNION ALL SELECT match_id, runs_total, delivery_id FROM ipl_deliveries
    ) T1
    JOIN (
        SELECT match_id, format FROM test_matches
        UNION ALL SELECT match_id, format FROM odi_matches
        UNION ALL SELECT match_id, format FROM t20_matches
        UNION ALL SELECT match_id, format FROM ipl_matches
    ) T2 ON T1.match_id = T2.match_id
    GROUP BY T1.match_id, T2.format
    HAVING COUNT(T1.delivery_id) > 60; -- Min 10 overs to ensure valid match
    """
    df_runrate = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.boxplot(x='format', y='RunRate', data=df_runrate, palette='viridis')
    plt.title('V2: Distribution of Match Run Rates by Format')
    plt.xlabel('Format')
    plt.ylabel('Run Rate (Runs per Over)')
    plt.savefig(os.path.join(VISUALS_DIR, 'V2_RunRate_Distribution.png'))
    plt.close()

def plot_3_dismissal_type_breakdown():
    """V3: Bar Chart - Frequency of Different Wicket Kinds (Dismissal Breakdown)."""
    print("Generating V3: Dismissal Type Breakdown...")
    query = """
    SELECT
        wicket_kind,
        COUNT(delivery_id) AS TotalDismissals
    FROM (
        SELECT delivery_id, wicket_kind FROM test_deliveries
        UNION ALL SELECT delivery_id, wicket_kind FROM odi_deliveries
        UNION ALL SELECT delivery_id, wicket_kind FROM t20_deliveries
        UNION ALL SELECT delivery_id, wicket_kind FROM ipl_deliveries
    )
    WHERE wicket_kind IS NOT NULL AND wicket_kind NOT IN ('retired hurt', 'not applicable')
    GROUP BY wicket_kind
    ORDER BY TotalDismissals DESC;
    """
    df_wickets = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.barplot(x='TotalDismissals', y='wicket_kind', data=df_wickets, palette='magma')
    plt.title('V3: Overall Frequency of Wicket Kinds')
    plt.xlabel('Total Dismissals')
    plt.ylabel('Dismissal Type')
    plt.savefig(os.path.join(VISUALS_DIR, 'V3_Wicket_Breakdown.png'))
    plt.close()

def plot_4_toss_win_match_win_correlation():
    """V4: Scatter Plot - Match Win Percentage based on Toss Decision."""
    print("Generating V4: Toss Win vs. Match Win Correlation...")
    # Using the pre-written SQL from analysis_queries.sql (Query 14)
    query = """
    SELECT
        toss_decision,
        CAST(SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(match_id) AS TossWinMatchWinPct
    FROM (
        SELECT match_id, toss_winner, winner, toss_decision FROM test_matches
        UNION ALL SELECT match_id, toss_winner, winner, toss_decision FROM odi_matches
        UNION ALL SELECT match_id, toss_winner, winner, toss_decision FROM t20_matches
        UNION ALL SELECT match_id, toss_winner, winner, toss_decision FROM ipl_matches
    )
    WHERE winner IS NOT 'No Result' AND toss_decision IS NOT NULL
    GROUP BY toss_decision;
    """
    df_toss = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.barplot(x='toss_decision', y='TossWinMatchWinPct', data=df_toss, palette='pastel')
    plt.title('V4: Match Win % When Winning Toss by Decision')
    plt.xlabel('Toss Decision')
    plt.ylabel('Match Win Percentage (%)')
    plt.ylim(45, 55) # Focus on the typical range around 50%
    plt.savefig(os.path.join(VISUALS_DIR, 'V4_Toss_Impact.png'))
    plt.close()

def plot_5_venue_vs_match_count():
    """V5: Bar Chart - Top 10 Venues by Total Matches Hosted."""
    print("Generating V5: Top 10 Venues by Match Count...")
    # Using the pre-written SQL from analysis_queries.sql (Query 16)
    query = """
    SELECT
        venue,
        COUNT(match_id) AS TotalMatchesHosted
    FROM (
        SELECT match_id, venue FROM test_matches
        UNION ALL SELECT match_id, venue FROM odi_matches
        UNION ALL SELECT match_id, venue FROM t20_matches
        UNION ALL SELECT match_id, venue FROM ipl_matches
    )
    GROUP BY venue
    ORDER BY TotalMatchesHosted DESC
    LIMIT 10;
    """
    df_venues = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.barplot(x='TotalMatchesHosted', y='venue', data=df_venues, palette='rocket')
    plt.title('V5: Top 10 Venues by Total Matches Hosted')
    plt.xlabel('Total Matches Hosted')
    plt.ylabel('Venue')
    plt.tight_layout()
    plt.savefig(os.path.join(VISUALS_DIR, 'V5_Venue_Match_Count.png'))
    plt.close()

def plot_6_batter_boundary_breakdown():
    """V6: Bar Chart - Top 10 Hitters: Sixes vs. Fours."""
    print("Generating V6: Top Hitters Boundary Breakdown...")
    query = """
    SELECT
        batter,
        SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS TotalFours,
        SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS TotalSixes
    FROM (
        SELECT batter, runs_batter FROM test_deliveries
        UNION ALL SELECT batter, runs_batter FROM odi_deliveries
        UNION ALL SELECT batter, runs_batter FROM t20_deliveries
        UNION ALL SELECT batter, runs_batter FROM ipl_deliveries
    )
    GROUP BY batter
    ORDER BY (TotalFours + TotalSixes) DESC
    LIMIT 10;
    """
    df_boundaries = pd.read_sql(text(query), ENGINE)
    df_melt = df_boundaries.set_index('batter').stack().reset_index()
    df_melt.columns = ['batter', 'BoundaryType', 'Count']
    
    plt.figure(figsize=(12, 7))
    sns.barplot(x='batter', y='Count', hue='BoundaryType', data=df_melt, palette='coolwarm')
    plt.title('V6: Top 10 Batsmen - Total Fours vs. Sixes (All Formats)')
    plt.xlabel('Batter')
    plt.ylabel('Boundary Count')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Boundary Type')
    plt.tight_layout()
    plt.savefig(os.path.join(VISUALS_DIR, 'V6_Boundary_Breakdown.png'))
    plt.close()

def plot_7_bowler_effectiveness_scatter():
    """V7: Scatter Plot - Bowler Total Wickets vs. Economy Rate."""
    print("Generating V7: Bowler Effectiveness Scatter...")
    query = """
    SELECT
        bowler,
        COUNT(CASE WHEN is_wicket = 1 AND wicket_kind NOT IN ('run out', 'retired hurt') THEN 1 ELSE NULL END) AS TotalWickets,
        CAST(SUM(runs_total) * 6.0 AS REAL) / COUNT(delivery_id) AS EconomyRate,
        COUNT(delivery_id) AS BallsBowled
    FROM (
        SELECT bowler, is_wicket, wicket_kind, runs_total, delivery_id FROM odi_deliveries
        UNION ALL SELECT bowler, is_wicket, wicket_kind, runs_total, delivery_id FROM t20_deliveries
    )
    GROUP BY bowler
    HAVING BallsBowled >= 500 -- Minimum balls for relevance
    """
    df_bowlers = pd.read_sql(text(query), ENGINE)
    
    plt.figure(figsize=(10, 8))
    # Color points based on the number of balls bowled (a proxy for career length)
    sns.scatterplot(x='EconomyRate', y='TotalWickets', size='BallsBowled', 
                    hue='BallsBowled', data=df_bowlers, palette='cool', sizes=(20, 200))
    plt.title('V7: Bowler Effectiveness (ODI/T20 Combined)')
    plt.xlabel('Economy Rate (Runs/Over)')
    plt.ylabel('Total Wickets')
    plt.legend(title='Balls Bowled', loc='upper right')
    plt.savefig(os.path.join(VISUALS_DIR, 'V7_Bowler_Effectiveness.png'))
    plt.close()

def plot_8_victory_margin_distribution_runs():
    """V8: Histogram - Distribution of Match Wins by Run Margin."""
    print("Generating V8: Victory Margin Distribution (Runs)...")
    query = """
    SELECT by_runs FROM (
        SELECT by_runs FROM test_matches WHERE by_runs > 0
        UNION ALL SELECT by_runs FROM odi_matches WHERE by_runs > 0
        UNION ALL SELECT by_runs FROM t20_matches WHERE by_runs > 0
        UNION ALL SELECT by_runs FROM ipl_matches WHERE by_runs > 0
    )
    """
    df_margins = pd.read_sql(text(query), ENGINE)
    
    plt.figure()
    sns.histplot(df_margins['by_runs'], bins=30, kde=True, color='skyblue')
    plt.title('V8: Distribution of Victory Margins (by Runs)')
    plt.xlabel('Margin of Victory (Runs)')
    plt.ylabel('Number of Matches')
    plt.savefig(os.path.join(VISUALS_DIR, 'V8_Run_Margin_Distribution.png'))
    plt.close()

def plot_9_team_extras_reliance():
    """V9: Bar Chart - Team Reliance on Extras (Extras as % of Total Score)."""
    print("Generating V9: Team Extras Reliance...")
    # Using the pre-written SQL from analysis_queries.sql (Query 17)
    query = """
    SELECT
        batting_team,
        SUM(runs_extras) AS TotalExtras,
        SUM(runs_total) AS TotalScore,
        CAST(SUM(runs_extras) AS REAL) * 100.0 / SUM(runs_total) AS ExtrasPercentage
    FROM (
        SELECT batting_team, runs_extras, runs_total FROM test_deliveries
        UNION ALL SELECT batting_team, runs_extras, runs_total FROM odi_deliveries
        UNION ALL SELECT batting_team, runs_extras, runs_total FROM t20_deliveries
        UNION ALL SELECT batting_team, runs_extras, runs_total FROM ipl_deliveries
    )
    GROUP BY batting_team
    HAVING TotalScore >= 10000
    ORDER BY ExtrasPercentage DESC
    LIMIT 10;
    """
    df_extras = pd.read_sql(text(query), ENGINE)

    plt.figure()
    sns.barplot(x='ExtrasPercentage', y='batting_team', data=df_extras, palette='autumn')
    plt.title('V9: Top 10 Teams by Reliance on Extras (Score from Extras as % of Total)')
    plt.xlabel('Extras Percentage (%)')
    plt.ylabel('Team')
    plt.savefig(os.path.join(VISUALS_DIR, 'V9_Extras_Reliance.png'))
    plt.close()

def plot_10_average_first_innings_score_by_format():
    """V10: Grouped Bar Chart - Average 1st Innings Score by Format and Gender."""
    print("Generating V10: Average 1st Innings Score by Format/Gender...")
    # Using the pre-written SQL from analysis_queries.sql (Query 15)
    query = """
    SELECT
        match_format,
        gender,
        CAST(SUM(Runs) AS REAL) / COUNT(DISTINCT match_id) AS AvgFirstInningsScore
    FROM (
        SELECT T2.format AS match_format, T2.gender, T1.match_id, SUM(T1.runs_total) AS Runs FROM test_deliveries T1 JOIN test_matches T2 ON T1.match_id = T2.match_id WHERE T1.inning = 1 GROUP BY T1.match_id, T2.gender
        UNION ALL
        SELECT T2.format AS match_format, T2.gender, T1.match_id, SUM(T1.runs_total) AS Runs FROM odi_deliveries T1 JOIN odi_matches T2 ON T1.match_id = T2.match_id WHERE T1.inning = 1 GROUP BY T1.match_id, T2.gender
        UNION ALL
        SELECT T2.format AS match_format, T2.gender, T1.match_id, SUM(T1.runs_total) AS Runs FROM t20_deliveries T1 JOIN t20_matches T2 ON T1.match_id = T2.match_id WHERE T1.inning = 1 GROUP BY T1.match_id, T2.gender
        UNION ALL
        SELECT T2.format AS match_format, T2.gender, T1.match_id, SUM(T1.runs_total) AS Runs FROM ipl_deliveries T1 JOIN ipl_matches T2 ON T1.match_id = T2.match_id WHERE T1.inning = 1 GROUP BY T1.match_id, T2.gender
    )
    GROUP BY match_format, gender
    ORDER BY match_format, gender;
    """
    df_avg_score = pd.read_sql(text(query), ENGINE)

    plt.figure()
    sns.barplot(x='match_format', y='AvgFirstInningsScore', hue='gender', data=df_avg_score, palette='husl')
    plt.title('V10: Average First Innings Score by Format and Gender')
    plt.xlabel('Match Format')
    plt.ylabel('Average First Innings Score')
    plt.legend(title='Gender')
    plt.savefig(os.path.join(VISUALS_DIR, 'V10_Avg_1st_Innings.png'))
    plt.close()


# --- Main Execution ---
if __name__ == "__main__":
    print("\n--- Starting Data Visualization Phase (10 Visuals) ---")
    
    # Check if the database file exists
    if not os.path.exists(DATABASE_PATH):
        print(f"\nERROR: Database file not found at: {DATABASE_PATH}")
        print("Please run 'transform_load.py' first to create the database.")
    else:
        # Run all plotting functions
        plot_1_match_volume_over_time()
        plot_2_run_rate_distribution_by_format()
        plot_3_dismissal_type_breakdown()
        plot_4_toss_win_match_win_correlation()
        plot_5_venue_vs_match_count()
        plot_6_batter_boundary_breakdown()
        plot_7_bowler_effectiveness_scatter()
        plot_8_victory_margin_distribution_runs()
        plot_9_team_extras_reliance()
        plot_10_average_first_innings_score_by_format()
        
        print(f"\n All 10 visualizations saved to the '{VISUALS_DIR}' directory.")
