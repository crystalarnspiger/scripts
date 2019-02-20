CREATE TABLE ams_topo.ams_file
(
  file_id SERIAL PRIMARY KEY,
  filename_web VARCHAR(200) NULL,
  filename_web_w_format VARCHAR(200) NULL,
  filepath_web VARCHAR(200) NULL,
  file_url_original_web VARCHAR(200) NULL,
  digitization_batch_id VARCHAR(200) NULL,
  sheetname VARCHAR(200) NULL,
  filename_geo VARCHAR(200) NULL,
  filepath_geo VARCHAR(200) NULL,
  oclc VARCHAR(15) NULL
);

CREATE TABLE ams_topo.ams_catalog
(
  catalog_id SERIAL PRIMARY KEY,
  oclc VARCHAR(15) NULL,
  lc_control_number VARCHAR(25) NULL,
  geographic_classification VARCHAR(50) NULL,
  name VARCHAR(100) NULL,
  title VARCHAR(100) NULL,
  alternate_title VARCHAR(100) NULL,
  series VARCHAR(100) NULL,
  published_location VARCHAR(50) NULL,
  published_year VARCHAR(10) NULL,
  physical_description VARCHAR(75) NULL,
  size VARCHAR(50) NULL,
  scale VARCHAR(25) NULL,
  geographic_name VARCHAR(100) NULL,
  notes VARCHAR(1000) NULL,
  collection_url VARCHAR(200) NULL,
  uniform_title VARCHAR(100) NULL,
  genre_or_form VARCHAR(50) NULL,
  lc_call_number VARCHAR(25) NULL
);

CREATE TABLE ams_topo.place_names
(
  place_id SERIAL PRIMARY KEY,
  name VARCHAR(50)
);

CREATE TABLE ams_topo.place_lookup
(
  pl_id SERIAL PRIMARY KEY,
  place_id INTEGER,
  file_id INTEGER
);
