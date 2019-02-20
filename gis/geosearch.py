import ast
import csv
import psycopg2
import requests
import time

from pymarc import MARCReader, marcxml

import local_constants


def read_table(cursor):
    all_coordinates = []
    y = 0
    with open('/files/file_count.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        cursor.execute(
            """SELECT ST_AsGeoJSON(ams_topo.ams_footprints.geom), ams_topo.place_names.name,
            ams_topo.ams_file.filename_web, ams_topo.ams_file.file_url_original_web,
            ams_topo.ams_file.file_id
            FROM ams_topo.ams_footprints
            JOIN ams_topo.ams_file ON ams_topo.ams_file.gid=ams_topo.ams_footprints.gid
            JOIN ams_topo.place_lookup ON ams_topo.ams_file.file_id=ams_topo.place_lookup.file_id
            JOIN ams_topo.place_names ON ams_topo.place_lookup.place_id=ams_topo.place_names.place_id
            """
        )
        print ('done reading file and geom tables')
        results = cursor.fetchall()
        for result in results:
            y += 1
            coor_list = (ast.literal_eval(result[0])['coordinates'])
            for item in coor_list:
                coordinates = item[0]
                min_latitude = coordinates[0][1]
                min_longitude = coordinates[0][0]
                max_latitude = coordinates[2][1]
                max_longitude = coordinates[2][0]
                all_coordinates.append(
                    {
                        'y': y,
                        'file_id': result[-1],
                        'min_latitude': min_latitude,
                        'min_longitude': min_longitude,
                        'max_latitude': max_latitude,
                        'max_longitude': max_longitude
                    }
                )
                print (
                    result[-1],
                    min_latitude,
                    min_longitude,
                    max_latitude,
                    max_longitude
                )
                row = [y, result[-1]]
                writer.writerow(row)
                print (row)

        print ('end of results')

    return all_coordinates


def geosearch(search_criteria):
    print ('starting search')
    completed_files = []
    table_entries = []
    with open('place_names.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        session = requests.Session()
        base_uri = 'https://places.mapzen.com/v1/?method=mapzen.places.getIntersects'
        api_key = local_constants.map_zen_api_key
        x = 0
        for item in search_criteria:
            y = item['y']
            file_id = item['file_id']
            print ('number: {0} file: {1}'.format(y, file_id))
            min_latitude = item['min_latitude']
            min_longitude = item['min_longitude']
            max_latitude = item['max_latitude']
            max_longitude = item['max_longitude']
            x += 1
            if x == 5:
                print (x)
                time.sleep(2)
                x = 0
            response = session.get(
                '{0}&api_key={1}&min_latitude={2}&min_longitude={3}&max_latitude={4}&max_longitude={5}'.format(
                    base_uri,
                    api_key,
                    min_latitude,
                    min_longitude,
                    max_latitude,
                    max_longitude
                )
            )
            try:
                json = response.json()
            except json.decoder.JSONDecodeError:
                print (response)
                print (file_id, min_latitude, min_longitude, max_latitude, max_longitude)
            try:
                results = json['places']
                for result in results:
                    if result['wof:placetype'] in (
                        'ocean',
                        'continent',
                        'country',
                        'locality',
                        'marinearea',
                        'dependency',
                        'disputed',
                        'macroregion',
                        'region'
                    ):
                        table_entries.append(
                            {
                                'file_id': file_id,
                                'place': result['wof:name']
                            }
                        )
                        row = [file_id, result['wof:name']]
                        writer.writerow(row)
                        print (row)
            except KeyError:
                print (json)
                print (
                    'file_id', file_id,
                    'min_latitude', min_latitude,
                    'min_longitude', min_longitude,
                    'max_latitude', max_latitude,
                    'max_longitude', max_longitude
                )

    return table_entries


def check_lookup():
    lookup_issues = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection for lookup')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT place_id
        FROM ams_topo.place_names"""
    )
    place_ids = cursor.fetchall()
    for place_id in place_ids:
        cursor.execute(
            """SELECT pl_id
            FROM ams_topo.place_lookup
            WHERE ams_topo.place_lookup.place_id = %s""",
            (place_id,)
        )
        try:
            lookup_id = cursor.fetchone()[0]
        except TypeError:
            lookup_issues.append(place_id)
    conn.close()
    print ('connection closed for lookup')

    return lookup_issues


def process_search(table_read=None):
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    if table_read:
        table_entries=[]
        with open('place_names.csv') as read_file:
            reader = csv.reader(read_file, delimiter=',')
            for row in reader:
                table_entries.append(
                    {
                        'file_id': row[0],
                        'place': row[1]
                    }
                )
    else:
        search_criteria = read_table(cursor)
        ('table read')
        table_entries = geosearch(search_criteria)
        print ('entries fetched')
    for entry in table_entries:
        place_id = ''
        lookup_id = ''
        cursor.execute(
            """SELECT place_id
            FROM ams_topo.place_names
            WHERE ams_topo.place_names.name = %s """,
            (entry['place'],)
        )
        try:
            place_id = cursor.fetchone()[0]
        except TypeError:
            pass
        if not place_id:
            cursor.execute(
                """INSERT INTO ams_topo.place_names
                (name) VALUES (%s) RETURNING place_id""",
                (entry['place'],)
            )
            place_id = cursor.fetchone()[0]
        print ('place_id: ', place_id)
        cursor.execute(
            """SELECT pl_id
            FROM ams_topo.place_lookup
            WHERE ams_topo.place_lookup.place_id = %s
            AND ams_topo.place_lookup.file_id = %s""",
            (place_id, entry['file_id'])
        )
        try:
            lookup_id = cursor.fetchone()[0]
        except TypeError:
            pass
        if not lookup_id:
            cursor.execute(
                """INSERT INTO ams_topo.place_lookup
                (place_id, file_id) VALUES (%s, %s)
                RETURNING ams_topo.place_lookup.pl_id""",
                (place_id, entry['file_id'])
            )
            lookup_id = cursor.fetchone()[0]
        print (lookup_id)
    print ('finished entries')
    # conn.commit()
    # print ('committed')
    print ('checking lookup')
    lookup_results = check_lookup()
    print (lookup_results)
