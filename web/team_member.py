import ast
import csv
import datetime
import mysql.connector
import psycopg2
import pymysql
import requests
import time
import urllib

import local_constants


def read_file():
    office_buildings = []
    records = []
    with open('../../Downloads/directory_information (1).csv') as read_file:
        print ('reading file')
        reader = csv.DictReader(read_file, delimiter='|')
        for record in reader:
            if record['utexasEduPersonBirthDate']:
                birthdate = datetime.datetime.strptime(record['utexasEduPersonBirthDate'], '%Y%m%d')
                # birthdate = birthdate.strftime('%m/%d/%Y')
                record['utexasEduPersonBirthDate'] = birthdate.strftime('%m/%d/%Y')
                records.append(record)
            # if record['utexasEduPersonOfficeBuilding'] not in office_buildings:
            #     office_buildings.append(record['utexasEduPersonOfficeBuilding'])
            # print (record['utexasEduPersonOfficeBuilding'])
            # if record['utexasEduPersonOfficeBuilding'] in ['UTA', 'PAR', 'PRC', 'CDL']:
            #     print ('**********', record['utexasEduPersonOfficeBuilding'], '***********')
            #     print (record)
        # print (office_buildings)
        print ('done reading')
    with open ('../../Downloads/directory_information_rewrite.csv', 'w') as write_file:
        print ('writing file')
        fieldnames = records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter='|')
        writer.writeheader()
        for record in records:
            writer.writerow(record)
        print ('done writing')


def direct_connect():
    print ('connecting')
    db = mysql.connector.connect(host=local_constants.drupal_host, user=local_constants.drupal_user, password=local_constants.drupal_password, db=local_constants.drupal_db, port=local_constants.drupal_port)
    cursor = db.cursor()
    print ('retrieving records')
    cursor.execute(
        """SELECT * FROM node
        WHERE node.type = %s;""",
        ('team_member',)
    )

    rows = cursor.fetchall()
    # print ('first record: {0}'.format(data))
    x = 1
    types = []
    for row in rows:
        print (row)
        if row[2] not in types:
            types.append(row[2])
        print (x)
        x += 1

    print (types)

    print ('closing connection')
    db.close()
