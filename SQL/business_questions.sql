-- Q1: Which Actors have starred in the most shows?
DO $$
BEGIN
    IF NOT EXISTS ( --- Check if materialized view exists before creating it.
       SELECT 1
       FROM pg_matviews
       WHERE matviewname = 'q1_top_actors_per_num_shows'
    ) THEN
       EXECUTE 'CREATE MATERIALIZED VIEW q1_top_actors_per_num_shows AS SELECT ca.actor, COUNT(*) as num_shows_starred FROM casting ca
                JOIN rt_show_casting rt ON ca.actor_id = rt.actor_id
                JOIN shows sh ON rt.show_id = sh.show_id
                GROUP BY ca.actor ORDER BY num_shows_starred DESC LIMIT 5';
    END IF;
END $$;

-- Q2: Number of shows release each year, by genre
DO $$
BEGIN
   IF NOT EXISTS (
   SELECT 1
   FROM pg_matviews
   WHERE matviewname = 'q2_num_shows_per_year_n_genre'
    ) THEN
        EXECUTE 'CREATE MATERIALIZED VIEW q2_num_shows_per_year_n_genre AS SELECT ge.genres, sh.release_year, COUNT(*) as num_shows FROM genres ge
                JOIN rt_show_genres rt ON ge.genre_id = rt.genre_id
                JOIN shows sh ON rt.show_id = sh.show_id
                GROUP BY ROLLUP (ge.genres, sh.release_year)
                ORDER BY ge.genres, sh.release_year DESC';
  END IF;
END $$;
    --Alternative grouping with Year over YEAR (YoY) difference
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1
       FROM pg_matviews
       WHERE matviewname = 'q2_num_shows_yoy_per_genre'
    ) THEN
        EXECUTE 'CREATE MATERIALIZED VIEW q2_num_shows_yoy_per_genre AS SELECT ge.genres,
                       sh.release_year,
                       COUNT(*) as num_shows,
                       COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY ge.genres ORDER BY sh.release_year) AS diff_shows_yoy
                FROM genres ge
                JOIN rt_show_genres rt ON ge.genre_id = rt.genre_id
                JOIN shows sh ON rt.show_id = sh.show_id
                GROUP BY ge.genres, sh.release_year
                ORDER BY ge.genres, sh.release_year DESC';
    END IF;
END $$;

-- Q3: Distribution of show durations by content rating (MOVIES only)
DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1
       FROM pg_matviews
       WHERE matviewname = 'q3_movie_dur_per_content_rating'
    ) THEN
       EXECUTE 'CREATE MATERIALIZED VIEW q3_movie_dur_per_content_rating AS SELECT
                    sub.rating,
                    ROUND(AVG(sub.duration_min), 2) AS avg_duration,
                    MIN(sub.duration_min) AS min_duration,
                    MAX(sub.duration_min) AS max_duration

                    FROM
                        -- This is just for processing the "duration" column which is in text format
                        (SELECT
                        sh.rating,
                        CAST(SPLIT_PART(sh.duration, '' '', 1) AS INT) AS duration_min
                        FROM shows sh
                        WHERE sh.type=''Movie'') sub
                    GROUP BY sub.rating';
   END IF;
END $$;

--Q4: TOP Directors by number of shows

DO $$
BEGIN
    IF NOT EXISTS (
       SELECT 1
       FROM pg_matviews
       WHERE matviewname = 'q4_top_directors_per_num_shows'
    ) THEN
       EXECUTE 'CREATE MATERIALIZED VIEW q4_top_directors_per_num_shows AS SELECT dr.director, COUNT(*) as num_shows FROM shows sh
                JOIN rt_show_directors rt ON sh.show_id = rt.show_id
                JOIN directors dr ON rt.director_id = dr.director_id
                GROUP BY dr.director
                ORDER BY num_shows DESC
                LIMIT 10';
   END IF;
END $$