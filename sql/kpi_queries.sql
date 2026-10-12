-- kpi_queries.sql
-- The KPI queries behind the dashboard (Section 8.1). All of them read the KPI
-- views defined in dw_schema.sql, which sit on top of the dimensional tables.

-- KPI-1 (R1): audio profile, nominated vs not (headline)
SELECT is_grammy_nominated, track_genre_rows,
       ROUND(avg_danceability::numeric, 3) AS danceability, ROUND(avg_energy::numeric, 3) AS energy,
       ROUND(avg_valence::numeric, 3) AS valence, ROUND(avg_acousticness::numeric, 3) AS acousticness,
       ROUND(avg_tempo::numeric, 1) AS tempo
FROM kpi_r1_audio_profile_overall ORDER BY is_grammy_nominated;

-- KPI-2 (R2): popularity of nominated vs non-nominated tracks
SELECT is_grammy_nominated, tracks, ROUND(avg_popularity::numeric, 1) AS avg_popularity,
       median_popularity, ROUND((share_popularity_50_plus * 100)::numeric, 1) AS pct_popularity_50_plus
FROM kpi_r2_popularity_summary ORDER BY is_grammy_nominated;

-- KPI-3 (R3): genres of nominated tracks by decade (top 10 overall)
SELECT decade, genre_name, nominations FROM kpi_r3_genre_by_decade ORDER BY nominations DESC, decade LIMIT 10;

-- Sensitivity check for R2 (precision of the integration): popularity of tracks matched EXACTLY
-- (title and full artist string equal) vs tracks matched through the normalized tier.
SELECT n.match_type, COUNT(DISTINCT n.track_key) AS tracks,
       ROUND(AVG(t.pop)::numeric, 1) AS avg_popularity
FROM fact_nomination n
JOIN (SELECT track_key, MAX(popularity) AS pop FROM fact_track GROUP BY track_key) t USING (track_key)
WHERE n.track_key IS NOT NULL
GROUP BY n.match_type ORDER BY n.match_type;
