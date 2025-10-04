-- PLAYER STATS
-- name: top_10_batsmen_ODI_runs
-- 1.Calculates the top 10 batsmen by total runs scored in ODI matches, filtered for players with at least 500 runs.
SELECT batter, SUM(runs_batter) AS TotalRuns
FROM odi_deliveries
GROUP BY batter
HAVING TotalRuns >= 500
ORDER BY TotalRuns DESC
LIMIT 10;

-- name: leading_wicket_takers_T20_total
-- 2.Finds the leading wicket-takers in combined T20/IPL matches, excluding run-outs and retired-hurt, filtered for players with at least 25 wickets.
SELECT
    bowler,
    COUNT(CASE WHEN is_wicket = 1 AND wicket_kind NOT IN ('run out', 'retired hurt') THEN 1 ELSE NULL END) AS TotalWickets
FROM (
    SELECT bowler, is_wicket, wicket_kind FROM t20_deliveries
    UNION ALL
    SELECT bowler, is_wicket, wicket_kind FROM ipl_deliveries
)
GROUP BY bowler
HAVING TotalWickets >= 25
ORDER BY TotalWickets DESC
LIMIT 10;

-- name: best_T20_career_avg
-- 3.Calculates the best career average runs per match in T20/IPL, filtered for players with at least 500 career runs.
WITH T20Data AS (
    SELECT match_id, batter, runs_batter
    FROM t20_deliveries
    UNION ALL
    SELECT match_id, batter, runs_batter
    FROM ipl_deliveries
)
SELECT
    batter,
    SUM(runs_batter) AS TotalRuns,
    CAST(SUM(runs_batter) AS REAL) / COUNT(DISTINCT match_id) AS AvgRunsPerMatch
FROM T20Data
GROUP BY batter
HAVING TotalRuns >= 500
ORDER BY AvgRunsPerMatch DESC
LIMIT 10;

-- name: best_test_economy
-- 4.Determines the best bowling economy rate (runs conceded per 6 balls/over) in Test matches, filtered for bowlers with at least 1000 balls bowled.
SELECT
    bowler,
    COUNT(delivery_id) AS BallsBowled,
    CAST(SUM(runs_total) * 6.0 AS REAL) / COUNT(delivery_id) AS EconomyRate
FROM test_deliveries
GROUP BY bowler
HAVING BallsBowled >= 1000
ORDER BY EconomyRate ASC
LIMIT 10;

-- name: most_career_sixes_all_formats
-- 5.Counts the total career sixes across all four formats (Test, ODI, T20, IPL), filtered for players with at least 50 sixes.
SELECT
    batter,
    COUNT(CASE WHEN runs_batter = 6 THEN 1 ELSE NULL END) AS TotalSixes
FROM (
    SELECT batter, runs_batter FROM test_deliveries
    UNION ALL
    SELECT batter, runs_batter FROM odi_deliveries
    UNION ALL
    SELECT batter, runs_batter FROM t20_deliveries
    UNION ALL
    SELECT batter, runs_batter FROM ipl_deliveries
)
GROUP BY batter
HAVING TotalSixes >= 50
ORDER BY TotalSixes DESC
LIMIT 10;

-- name: total_centuries_approx_all_formats
-- 6.Counts the total number of approximate centuries (100+ runs in an innings) for players across all formats.
WITH AllScores AS (
    SELECT match_id, inning, batter, SUM(runs_batter) AS InningsScore FROM test_deliveries GROUP BY match_id, inning, batter
    UNION ALL
    SELECT match_id, inning, batter, SUM(runs_batter) AS InningsScore FROM odi_deliveries GROUP BY match_id, inning, batter
    UNION ALL
    SELECT match_id, inning, batter, SUM(runs_batter) AS InningsScore FROM t20_deliveries GROUP BY match_id, inning, batter
    UNION ALL
    SELECT match_id, inning, batter, SUM(runs_batter) AS InningsScore FROM ipl_deliveries GROUP BY match_id, inning, batter
)
SELECT
    batter,
    COUNT(*) AS TotalCenturies
FROM AllScores
WHERE InningsScore >= 100
GROUP BY batter
ORDER BY TotalCenturies DESC
LIMIT 10;

-- name: most_bowled_wickets
-- 7.Finds the bowlers with the most 'bowled' dismissals across all formats.
SELECT
    bowler,
    COUNT(*) AS BowledWickets
FROM (
    SELECT bowler, wicket_kind FROM test_deliveries
    UNION ALL
    SELECT bowler, wicket_kind FROM odi_deliveries
    UNION ALL
    SELECT bowler, wicket_kind FROM t20_deliveries
    UNION ALL
    SELECT bowler, wicket_kind FROM ipl_deliveries
)
WHERE wicket_kind = 'bowled'
GROUP BY bowler
ORDER BY BowledWickets DESC
LIMIT 10;

-- name: highest_t20_strike_rate
-- 8.Calculates the highest T20/IPL strike rate (runs per 100 balls), filtered for batsmen who have faced at least 500 balls.
SELECT
    batter,
    CAST(SUM(runs_batter) * 100.0 AS REAL) / SUM(CASE WHEN extras_type IS NULL THEN 1 ELSE 0 END) AS StrikeRate
FROM (
    SELECT batter, runs_batter, extras_type FROM t20_deliveries
    UNION ALL
    SELECT batter, runs_batter, extras_type FROM ipl_deliveries
)
GROUP BY batter
HAVING SUM(CASE WHEN extras_type IS NULL THEN 1 ELSE 0 END) >= 500
ORDER BY StrikeRate DESC
LIMIT 10;

-- name: most_player_of_match_awards
-- 9.Lists the players with the most 'Player of the Match' awards across all formats.
SELECT
    player_of_match AS Player,
    COUNT(*) AS TotalAwards
FROM (
    SELECT player_of_match FROM test_matches
    UNION ALL
    SELECT player_of_match FROM odi_matches
    UNION ALL
    SELECT player_of_match FROM t20_matches
    UNION ALL
    SELECT player_of_match FROM ipl_matches
)
WHERE player_of_match IS NOT NULL AND player_of_match != ''
GROUP BY player_of_match
ORDER BY TotalAwards DESC
LIMIT 10;

-- name: top_bowler_batter_dismissals
--                                            10.Identifies the most frequent bowler-batsman dismissal pair across all formats.
SELECT
    bowler,
    player_out AS Batter,
    COUNT(delivery_id) AS TimesDismissed
FROM (
    SELECT delivery_id, bowler, player_out, is_wicket, wicket_kind FROM test_deliveries
    UNION ALL
    SELECT delivery_id, bowler, player_out, is_wicket, wicket_kind FROM odi_deliveries
    UNION ALL
    SELECT delivery_id, bowler, player_out, is_wicket, wicket_kind FROM t20_deliveries
    UNION ALL
    SELECT delivery_id, bowler, player_out, is_wicket, wicket_kind FROM ipl_deliveries
)
WHERE is_wicket = 1
  AND wicket_kind NOT IN ('run out', 'retired hurt')
  AND player_out IS NOT NULL
GROUP BY bowler, player_out
ORDER BY TimesDismissed DESC
LIMIT 10;

-- TEAM & MATCH STATS
-- name: highest_win_pct_test_cricket
-- 11.Approximates the highest winning percentage in Test cricket (Win Count / Total Matches Played), filtered for teams that have played at least 10 matches.
WITH AllTestTeams AS (
    SELECT match_id, winner, teams_str
    FROM test_matches
    WHERE winner IS NOT 'No Result' AND winner IS NOT NULL
)
SELECT
    T1.winner AS Team,
    CAST(COUNT(T1.match_id) AS REAL) * 100 / (
        SELECT COUNT(T2.match_id) FROM AllTestTeams T2 WHERE T2.teams_str LIKE '%' || T1.winner || '%'
    ) AS WinPercentage
FROM AllTestTeams T1
GROUP BY T1.winner
HAVING COUNT(T1.match_id) >= 10
ORDER BY WinPercentage DESC;

-- name: narrowest_victory_by_runs
-- 12.Lists the 10 narrowest victories determined by run margin across all formats (where the run margin is greater than 0).
SELECT
    format,
    winner,
    by_runs,
    date,
    venue
FROM (
    SELECT format, winner, by_runs, date, venue FROM test_matches
    UNION ALL
    SELECT format, winner, by_runs, date, venue FROM odi_matches
    UNION ALL
    SELECT format, winner, by_runs, date, venue FROM t20_matches
    UNION ALL
    SELECT format, winner, by_runs, date, venue FROM ipl_matches
)
WHERE by_runs > 0
ORDER BY by_runs ASC
LIMIT 10;

-- name: narrowest_victory_by_wickets
-- 13.Lists the 10 narrowest victories determined by wicket margin across all formats (where the wicket margin is greater than 0).
SELECT
    format,
    winner,
    by_wickets,
    date,
    venue
FROM (
    SELECT format, winner, by_wickets, date, venue FROM test_matches
    UNION ALL
    SELECT format, winner, by_wickets, date, venue FROM odi_matches
    UNION ALL
    SELECT format, winner, by_wickets, date, venue FROM t20_matches
    UNION ALL
    SELECT format, winner, by_wickets, date, venue FROM ipl_matches
)
WHERE by_wickets > 0
ORDER BY by_wickets ASC
LIMIT 10;

-- name: toss_win_match_win_pct
-- 14.Calculate the percentage of matches won by the team that wins the toss.
SELECT
    toss_decision,
    CAST(SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(match_id) AS TossWinMatchWinPct
FROM (
    SELECT match_id, toss_winner, winner, toss_decision FROM test_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, toss_decision FROM odi_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, toss_decision FROM t20_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, toss_decision FROM ipl_matches
)
WHERE winner IS NOT 'No Result' AND toss_decision IS NOT NULL
GROUP BY toss_decision;

-- name: avg_first_innings_score
-- 15.Calculates the average first innings score, grouped by match format and gender.
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

-- name: most_matches_hosted_venues
-- 16.Finds the top 10 venues that have hosted the most international and IPL matches.
SELECT
    venue,
    COUNT(match_id) AS TotalMatchesHosted
FROM (
    SELECT match_id, venue FROM test_matches
    UNION ALL
    SELECT match_id, venue FROM odi_matches
    UNION ALL
    SELECT match_id, venue FROM t20_matches
    UNION ALL
    SELECT match_id, venue FROM ipl_matches
)
GROUP BY venue
ORDER BY TotalMatchesHosted DESC
LIMIT 10;

-- name: team_extras_reliance_pct
-- 17.Calculates the percentage of total team score that comes from extras, filtered for teams with a high total score (10,000 runs).
SELECT
    batting_team,
    SUM(runs_extras) AS TotalExtras,
    SUM(runs_total) AS TotalScore,
    CAST(SUM(runs_extras) AS REAL) * 100.0 / SUM(runs_total) AS ExtrasPercentage
FROM (
    SELECT batting_team, runs_extras, runs_total FROM test_deliveries
    UNION ALL
    SELECT batting_team, runs_extras, runs_total FROM odi_deliveries
    UNION ALL
    SELECT batting_team, runs_extras, runs_total FROM t20_deliveries
    UNION ALL
    SELECT batting_team, runs_extras, runs_total FROM ipl_deliveries
)
GROUP BY batting_team
HAVING TotalScore >= 10000
ORDER BY ExtrasPercentage DESC
LIMIT 10;

-- name: yearly_match_trend
-- 18.Tracks the total number of matches played across all formats per year to show trends over time.
SELECT
    CAST(SUBSTR(date, 1, 4) AS INTEGER) AS MatchYear,
    COUNT(match_id) AS MatchesPlayed
FROM (
    SELECT match_id, date FROM test_matches
    UNION ALL
    SELECT match_id, date FROM odi_matches
    UNION ALL
    SELECT match_id, date FROM t20_matches
    UNION ALL
    SELECT match_id, date FROM ipl_matches
)
GROUP BY MatchYear
ORDER BY MatchYear;

-- name: home_win_ratio_approximation
-- 19.Approximates the home win ratio by looking at matches where the toss-winning team's name is in the venue name, filtered for teams with at least 10 "home" tosses.
SELECT
    toss_winner AS Team,
    COUNT(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) AS Wins,
    COUNT(match_id) AS TotalTosses
FROM (
    SELECT match_id, toss_winner, winner, venue FROM test_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, venue FROM odi_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, venue FROM t20_matches
    UNION ALL
    SELECT match_id, toss_winner, winner, venue FROM ipl_matches
)
WHERE venue LIKE '%' || toss_winner || '%'
GROUP BY toss_winner
HAVING TotalTosses >= 10
ORDER BY Wins * 1.0 / TotalTosses DESC
LIMIT 10;

-- name: highest_total_match_score
-- 20.Finds the top 5 matches with the highest combined total runs scored across all innings and formats.
SELECT
    match_id,
    TotalMatchScore
FROM (
    SELECT T1.match_id, SUM(T1.runs_total) AS TotalMatchScore FROM test_deliveries T1 GROUP BY T1.match_id
    UNION ALL
    SELECT T1.match_id, SUM(T1.runs_total) AS TotalMatchScore FROM odi_deliveries T1 GROUP BY T1.match_id
    UNION ALL
    SELECT T1.match_id, SUM(T1.runs_total) AS TotalMatchScore FROM t20_deliveries T1 GROUP BY T1.match_id
    UNION ALL
    SELECT T1.match_id, SUM(T1.runs_total) AS TotalMatchScore FROM ipl_deliveries T1 GROUP BY T1.match_id
)
ORDER BY TotalMatchScore DESC
LIMIT 5;
