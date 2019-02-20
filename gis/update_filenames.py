import ast
import csv
import psycopg2
import requests


def read_table():
    results = []
    conn = psycopg2.connect("dbname='gis' user=local_constants.gis_db_user host=local_constants.gis_db_host password=local_constants.gis_db_password")
    print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT sheet_id, filepath_geo
        FROM ams_topo.ams_sheets
        WHERE ams_topo.ams_sheets.filepath_geo LIKE CONCAT('%', 'modified', '%')"""
    )
    print ('done reading table')

    sheets = cursor.fetchall()
    for sheet in sheets:
        print (sheet[1])
        updated_filepath = sheet[1].replace('modified', 'r')
        print (updated_filepath)
        cursor.execute(
            """UPDATE ams_topo.ams_sheets
            SET filepath_geo = (%s)
            WHERE (sheet_id = %s)
            RETURNING sheet_id""",
            (updated_filepath, sheet[0])
        )
        print (cursor.fetchone()[0], 'updated with', updated_filepath)

    print ('committing changes')
    conn.commit()

    print ('done')
