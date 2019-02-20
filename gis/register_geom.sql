CREATE TABLE relaciones_geograficas.points
(
  id SERIAL PRIMARY KEY,
  geom GEOMETRY(Point, 4326)
);
