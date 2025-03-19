-- This code is to generate table relationships and set up primary keys and other constrains

BEGIN; -- Execute all code between "BEGIN" and "COMMIT" as a unique block. Prevents partial execution of code if there's an error.

-- ADD PRIMARY KEYS
    -- FACT and DIMENSION tables
ALTER TABLE shows ADD PRIMARY KEY (show_id);
ALTER TABLE directors ADD PRIMARY KEY (director_id);
ALTER TABLE countries ADD PRIMARY KEY (country_id);
ALTER TABLE casting ADD PRIMARY KEY (actor_id);
ALTER TABLE genres ADD PRIMARY KEY (genre_id);

    -- Relational (Intermediate tables)
ALTER TABLE rt_show_directors ADD PRIMARY KEY (show_id, director_id);
ALTER TABLE rt_show_countries ADD PRIMARY KEY (show_id, country_id);
ALTER TABLE rt_show_casting ADD PRIMARY KEY (show_id, actor_id);
ALTER TABLE rt_show_genres ADD PRIMARY KEY (show_id, genre_id);

-- ADD FOREIGN KEYS and CONSTRAINTS

ALTER TABLE rt_show_directors ADD FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE rt_show_directors ADD FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE rt_show_countries ADD FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE rt_show_countries ADD FOREIGN KEY (country_id) REFERENCES countries(country_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE rt_show_casting ADD FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE rt_show_casting ADD FOREIGN KEY (actor_id) REFERENCES casting(actor_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE rt_show_genres ADD FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE rt_show_genres ADD FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE ON UPDATE CASCADE;


COMMIT;