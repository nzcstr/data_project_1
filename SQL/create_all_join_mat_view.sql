BEGIN;
CREATE MATERIALIZED VIEW IF NOT EXISTS mat_view_all_join AS

	SELECT sh.*, dr.director, ca.actor, co.country, ge.genres FROM
	shows sh
	JOIN rt_show_directors rt1 ON sh.show_id = rt1.show_id
	JOIN directors dr ON rt1.director_id = dr.director_id

	JOIN rt_show_casting rt2 ON sh.show_id = rt2.show_id
	JOIN casting ca ON rt2.actor_id = ca.actor_id

	JOIN rt_show_countries rt3 ON sh.show_id = rt3.show_id
	JOIN countries co ON rt3.country_id = co.country_id

	JOIN rt_show_genres rt4 ON sh.show_id = rt4.show_id
	JOIN genres ge ON rt4.genre_id = ge.genre_id

	WITH NO DATA;

REFRESH MATERIALIZED VIEW mat_view_all_join;

COMMIT;

