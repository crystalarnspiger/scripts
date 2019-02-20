import ast
import csv
import psycopg2
import requests
import time

from pymarc import MARCReader, marcxml
from xml.etree import ElementTree as etree


def xml_upload():
    conn = psycopg2.connect("dbname='gis' user=local_constants.gis_db_user host=local_constants.gis_db_host password=local_constants.gis_db_password")
    print ('successful connection')
    filename = './files/RG_Points_xml.xml'
    cursor = conn.cursor()
    tree = etree.parse(filename)
    print (tree)
    root = tree.getroot()
    print (root)
    print (root.attrib)
    for item in root:
        print ('item', item)
        for item2 in item:
            print ('item2', item2)
            print (item2.attrib)
            print (item2.text)
            for item3 in item2:
                print ('item3', item3)
                print (item3.attrib)
                print (item3.text)

    conn.commit()


def testupload():
    conn = psycopg2.connect("dbname=gis_test_db_user user=local_constants.gis_test_db_user host=local_constants.gis_test_db_host password=local_constants.gis_test_db_password")
    print ('successful connection')
    cursor = conn.cursor()
    with open('../../Documents/gis/Relaciones_Geograficas.csv') as read_file:
        reader = csv.DictReader(read_file, delimiter=',')
        for row in reader:
            print (row)
            if not row['Latitude']:
                break
            trans_sent = False
            if row['trans_sent'].lower() == 'X'.lower():
                trans_sent = True
            spoken_lang = row['spoken_lang'].split(',')
            cursor.execute(
                """INSERT INTO carnspi.rg_mmd_shapeload
                (
                trans_sent, uncorrected_on_ftp, call_numl, historical_town,
                modern_town, latitude, longitude, image_url, diocese, census,
                year, day_month, author, source, collection, text, text_link,
                spoken_lang, data, data_link, artist, map, map_link,
                presentation, presentation_link
                )
                VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
                )
                RETURNING id""",
                (
                trans_sent,
                row['uncorrected_on_FTP'],
                row['call_number'],
                row['historical_town'],
                row['modern_town'],
                row['Latitude'],
                row['Longtitude'],
                row['image'],
                row['diocese'],
                row['census_no'],
                row['year'],
                row['date'],
                row['author'],
                row['sources'],
                row['collection'],
                row['text'],
                row['text_link'],
                spoken_lang,
                row['data'],
                row['data_link'],
                row['artist'],
                row['map'],
                row['map_link'],
                row['presentation'],
                row['presentation_link']
                )
            )
            place_id = cursor.fetchone()[0]
    conn.commit()


def create_geom():
    conn = psycopg2.connect("dbname=gis_test_db_user user=local_constants.gis_test_db_user host=local_constants.gis_test_db_host password=local_constants.gis_test_db_password")
    print ('successful connection')
    cursor = conn.cursor()
    cursor.execute(
        """
            CREATE TABLE relaciones_geograficas.points
            (
              id SERIAL PRIMARY KEY,
              geom GEOMETRY(Point, 3857)
            );
        """
    )
    cursor.execute(
        """
            ALTER TABLE relaciones_geograficas.points
            ADD COLUMN cid INTEGER,
            ADD CONSTRAINT fk_c_id
            FOREIGN KEY (cid)
            REFERENCES relaciones_geograficas.collection_script_load(id);
        """
    )
    conn.commit()


def add_geom():
    connect_string = "dbname={0} user={1} host={2} password={3}".format(
        local_constants.gis_test_db_name,
        local_constants.gis_test_db_user,
        local_constants.gis_test_db_host,
        local_constants.gis_test_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    cursor.execute(
        """
            ALTER TABLE carnspi.rg_mmd_shapeload
            ADD COLUMN geom GEOMETRY(Point, 3857);
        """
    )
    cursor.execute(
        """
            SELECT
            carnspi.rg_mmd_shapeload.id,
            carnspi.rg_mmd_shapeload.latitude,
            carnspi.rg_mmd_shapeload.longitude
            FROM carnspi.rg_mmd_shapeload
            ;
        """
    )
    entries = cursor.fetchall()
    x = 0
    for entry in entries:
        print(entry)
        print('id', entry[0])
        print('lat', str(entry[1]))
        print('long', str(entry[2]))
        cid = entry[0]
        latitude = entry[1]
        longitude = entry[2]
        cursor.execute(
            """UPDATE carnspi.rg_mmd_shapeload
            SET geom = ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), 3857)
            WHERE carnspi.rg_mmd_shapeload.id = %s
            RETURNING id;
            """,
            (
                longitude,
                latitude,
                cid
            )
        )
        entry_id = cursor.fetchone()[0]
        print (entry_id)
    conn.commit()
