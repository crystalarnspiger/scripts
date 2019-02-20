ALTER TABLE ams_topo.ams_file
ADD COLUMN catalog_id INTEGER,
ADD CONSTRAINT fk_catalog_id
FOREIGN KEY (catalog_id)
REFERENCES ams_topo.ams_catalog(catalog_id);

ALTER TABLE ams_topo.place_lookup
ADD CONSTRAINT fk_place_id
FOREIGN KEY (place_id)
REFERENCES ams_topo.place_names(place_id);

ALTER TABLE ams_topo.place_lookup
ADD CONSTRAINT fk_file_id
FOREIGN KEY (file_id)
REFERENCES ams_topo.ams_file(file_id);

ALTER TABLE ams_topo.ams_file
ADD COLUMN gid INTEGER,
ADD CONSTRAINT fk_footprints_id
FOREIGN KEY (gid)
REFERENCES ams_topo.ams_footprints(gid);
