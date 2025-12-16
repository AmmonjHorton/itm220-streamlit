USE `scripture_st`;
CREATE OR REPLACE VIEW timeline AS
SELECT
    ss.volume,
    ss.book,
    ss.chapter,
    ss.verse,
    yi.year_of_event,
    yi.age,
    CASE
        WHEN yi.age = 'BC' THEN -yi.year_of_event
        ELSE yi.year_of_event
    END AS timeline_year,
    d.doctrine_name
FROM scripture_study AS ss
JOIN year_info AS yi
    ON ss.id = yi.scripture_study_id
JOIN doctrine AS d
    ON ss.id = d.scripture_study_id;


