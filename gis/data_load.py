import csv
import psycopg2

from pymarc import MARCReader, marcxml

import local_constants


def load_filedata():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
        local_constants.gis_db_user,
        local_constants.gis_db_host,
        local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()
    with open('../../../../Volumes/gis/pclmaps_working_georefed_cea.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            cur.execute(
                """INSERT INTO ams_topo.ams_file
                   (filename_web, filename_web_w_format, filepath_web,
                   file_url_original_web, digitization_batch_id,
                   filename_geo, filepath_geo, oclc)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                   (
                    row['File_Name'], row['File_With_Format'], row['Filepath'],
                    row['URL_Files'], row['Digitization_Batch_ID'],
                    row['GEOFile'], row['GEOFilePath'], row['OCLC']
                   )
            )
        print ('done reading')
    conn.commit()
    print ('committed')


def load_marcdata():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()

    spreadsheet_oclc = []
    with open('../../../../Volumes/gis/pclmaps_working_georefed_cea.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            spreadsheet_oclc.append(row['OCLC'])

    found_oclc = []
    with open('./gis_marc_read/mapsgis.mrc', 'rb') as read_file:
        reader= MARCReader(read_file)
        for record in reader:
            if record['001']:
                oclc = str(record['001'])
                oclc = oclc.split(' ')[-1]
                oclc = oclc.replace('ocm', '')
                oclc = oclc.replace('ocn', '')
                if oclc.startswith('0'):
                    oclc = oclc[1:]
                if oclc in spreadsheet_oclc:
                    control_number = ''
                    call_number = ''
                    geographic_classification = ''
                    name = ''
                    title = ''
                    alternate_title = ''
                    series = ''
                    published_location = ''
                    published_year = ''
                    physical_description = ''
                    size = ''
                    scale = ''
                    notes = ''
                    geographic_name = ''
                    url = ''
                    uniform_title = ''
                    genre = ''

                    if record['010']:
                        control_number = record['010']['a']
                    if record['050']:
                        call_number = '{0}{1}'.format(record['050']['a'], record['050']['b'])
                    if record['052']:
                        geographic_classification = record['052']['a']
                    if record['110']:
                        name = '{0} {1}'.format(record['110']['a'], record['110']['b'])
                    if record['245']:
                        title = record['245']['a']
                    if record['246']:
                        alternate_title = record['246']['a']
                    if record['490']:
                        series = record['490']['a']
                    if record['260']:
                        published_location = record['260']['a'].strip(':').strip(',').strip('[]')
                        published_year = record['260']['c'].strip('-')
                    if record['300']:
                        physical_description = record['300']['a'].strip(':')
                        size = record['300']['c']
                    if record['507']:
                        scale = record['507']['a']
                    if record['500']:
                        notes = ''
                        lines = record.get_fields('500')
                        for line in lines:
                            notes += line['a']
                    if record['651']:
                        geographic_name = '{0} {1}'.format(record['651']['a'], record['651']['v'])
                    if record['856']:
                        url = record['856']['u']
                    if record['830']:
                        uniform_title = '{0} {1}'.format(record['830']['a'], record['830']['v'])
                    if record['655']:
                        genre = '{0} {1}'.format(record['655']['a'], record['655']['v'])
                    cur.execute(
                        """INSERT INTO ams_topo.ams_catalog
                           (oclc, lc_control_number, lc_call_number,
                           geographic_classification, name, title,
                           alternate_title, series, published_location,
                           published_year, physical_description, size, scale,
                           notes, geographic_name, collection_url,
                           uniform_title, genre_or_form)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                           (
                            oclc, control_number, call_number,
                            geographic_classification, name, title,
                            alternate_title, series, published_location,
                            published_year, physical_description, size,
                            scale, notes, geographic_name,
                            url, uniform_title, genre
                           )
                    )
                    print ('record entered')
        print ('done reading')
    conn.commit()
    print ('committed')


def connect_file_to_catalog():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()
    cur.execute(
        """SELECT catalog_id, oclc
        FROM ams_topo.ams_catalog"""
    )
    catalog_entries = cur.fetchall()
    for entry in catalog_entries:
        cur.execute(
            """UPDATE ams_topo.ams_file
            SET catalog_id = %s
            WHERE oclc = %s""",
            (entry[0], entry[1])
        )
        print (entry)
    print ('done with update')
    conn.commit()
    print ('committed')


def add_starter_places():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()
    cur.execute(
        """SELECT title, alternate_title, geographic_name, catalog_id
        FROM ams_topo.ams_catalog"""
    )
    catalog_entries = cur.fetchall()
    print ('catalog entries fetched')
    for entry in catalog_entries:
        if entry[3] == 8:
            continue
        at1 = ''
        title = ''
        geographic_name = ''
        place_ids = []
        try:
            title = entry[0].rstrip('.:2501,')
            if title:
                cur.execute(
                    """INSERT INTO ams_topo.place_names (name)
                    VALUES (%s) RETURNING place_id""",
                    (title,)
                )
                place_ids.append(cur.fetchone()[0])
        except psycopg2.IntegrityError:
            pass
        try:
            if entry[1] == 'Finland-Scandinavia':
                at1 = 'Finland'
                cur.execute(
                    """INSERT INTO ams_topo.place_names (name)
                    VALUES (%s) RETURNING place_id""",
                    (at1,)
                )
                place_ids.append(cur.fetchone()[0])
        except psycopg2.IntegrityError:
            pass
        try:
            geographic_name = entry[2].rstrip('Maps.')
            if geographic_name:
                cur.execute(
                    """INSERT INTO ams_topo.place_names (name)
                    VALUES (%s) RETURNING place_id""",
                    (geographic_name,)
                )
                place_ids.append(cur.fetchone()[0])
        except psycopg2.IntegrityError:
            pass
        print (title, at1, geographic_name)
        print (entry[3])
        cur.execute(
            """SELECT file_id FROM ams_topo.ams_file
            WHERE catalog_id = %s""",
            (str(entry[3]))
        )
        file_entries = cur.fetchall()
        print ('file entries fetched')
        for entry in file_entries:
            print (entry)
            for place_id in place_ids:
                cur.execute(
                    """INSERT INTO ams_topo.place_lookup
                    (place_id, file_id)
                    VALUES (%s, %s)""",
                    (place_id, entry[0])
                )
        print ('place lookups entered')
    conn.commit()
    print ('committed')


def add_problem_filethings():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO ams_topo.place_names (name)
        VALUES (%s) RETURNING place_id""",
        ('Asia',)
    )
    place_id = cur.fetchone()[0]
    x = 1
    while x <=79:
        cur.execute(
            """INSERT INTO ams_topo.place_lookup
            (place_id, file_id)
            VALUES (%s, %s)""",
            (place_id, x)
        )
        x+=1
        print (place_id, x)
    conn.commit()


def connect_footprints_to_file():
    spreadsheet_files = []
    with open('../../../../Volumes/gis/pclmaps_working_georefed_cea.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            spreadsheet_files.append(row['GEOFile'].rstrip('.tif'))

    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cur = conn.cursor()
    cur.execute(
        """SELECT gid, name FROM ams_topo.ams_footprints"""
    )
    footprints = cur.fetchall()
    x = 1
    for entry in footprints:
        if entry[1] in spreadsheet_files:
            cur.execute(
                """UPDATE ams_topo.ams_file
                SET gid = %s
                WHERE filename_geo = CONCAT(%s, '.tif')""",
                (entry[0], entry[1])
            )
        else:
            print ('not found: {0} {1} {2}'.format(entry[0], entry[1], x))
            x += 1
    conn.commit()
    print ('changes committed')
