BEGIN;
    COPY shows TO '/app/exports/shows.csv' DELIMITER ',' CSV HEADER;
    COPY directors TO '/app/exports/directors.csv' DELIMITER ',' CSV HEADER;
    COPY casting TO '/app/exports/casting.csv' DELIMITER ',' CSV HEADER;
    COPY genres TO '/app/exports/genres.csv' DELIMITER ',' CSV HEADER;
    COPY countries TO '/app/exports/countries.csv' DELIMITER ',' CSV HEADER;

    COPY rt_show_directors TO '/app/exports/rt_show_directors.csv' DELIMITER ',' CSV HEADER;
    COPY rt_show_casting TO '/app/exports/rt_show_casting.csv' DELIMITER ',' CSV HEADER;
    COPY rt_show_genres TO '/app/exports/rt_show_genres.csv' DELIMITER ',' CSV HEADER;
    COPY rt_show_countries TO '/app/exports/rt_show_countries.csv' DELIMITER ',' CSV HEADER;
    COPY mat_view_all_join TO '/app/exports/mat_view_all_join.csv' DELIMITER ',' CSV HEADER;
COMMIT;
