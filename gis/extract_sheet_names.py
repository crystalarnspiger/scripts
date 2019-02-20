import ast
import csv
import psycopg2
import requests

import local_constants


def read_table():
    rows = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        catalog_id,
        file_id,
        filename_web
        FROM ams_topo.ams_file"""
    )
    file_records = cursor.fetchall()
    print ('records retrieved')
    for file_record in file_records:
        print (file_record)
        row = [
            file_record[0],
            file_record[1],
            file_record[2]
        ]
        name_split = file_record[2].split('-')
        for element in name_split:
            row.append(element)
        rows.append(row)
    print ('done constructing dict')
    print ('writing file')
    with open('sheetnames.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for row in rows:
            print ('writing row', row)
            writer.writerow(row)
    print ('done writing file')


def update_sheet_names():
    print ('connecting to database')
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    print ('opening file')
    with open('sheetnames.csv', 'r') as readfile:
        reader = csv.reader(readfile)
        print ('processing rows')
        for row in reader:
            name = ''
            file_id = row[1]
            name = '-'.join(row[3:]).rstrip('-').title()
            print ('updating', file_id, 'with', name)
            cursor.execute(
                """UPDATE ams_topo.ams_file
                SET sheet_name = (%s)
                WHERE (file_id = %s)
                RETURNING file_id""",
                (name, file_id)
            )
            print ('file_id: ', cursor.fetchone()[0], 'updated')
        print ('done reading file')
    print ('committing changes')
    conn.commit()
    print ('done with update')
