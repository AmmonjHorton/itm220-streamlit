USE `scripture_st`;
CREATE VIEW timeline AS 
SELECT ss.volume, ss.book, ss.chapter, ss.verse, yi.year_of_event, yi.age, d.doctrine_name
FROM scripture_study AS ss
JOIN year_info AS yi
ON ss.id = yi.scripture_study_id
JOIN doctrine AS d
ON ss.id = d.scripture_study_id;

