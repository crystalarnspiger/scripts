# import base64, hashlib, hmac
# from datetime import datetime
# import urllib2
# from urllib import unquote_plus
# import
import ast
import csv
import psycopg2
import requests

import local_constants


def read_table():
    results = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT DISTINCT(sierra_id)
        FROM ams_topo.ams_catalog"""
    )
    sierra_ids = cursor.fetchall()
    for sierra_id in sierra_ids:
        results.append(sierra_id[0])
    print ('done reading table')

    return results


def search_sierra(api_key, api_secret, id_list):
    results = []
    print ('getting token')
    get_token = requests.post(
        'https://catalog2.lib.utexas.edu/iii/sierra-api/v4/token',
        auth=(api_key, api_secret)
    ).json()
    api_token = get_token['access_token']
    print ('token received')
    print ('calling api with ids')
    for sierra_id in id_list:
        print ('retrieving record', sierra_id)
        results.append(requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/{0}?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields'.format(sierra_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json())

    print ('done reading api')

    return results


def callsierra():
    sierra_ids = read_table()
    print ('sierra id retrieved')
    api_key = local_constants.sierra_api_key
    api_secret = local_constants.sierra_api_secret
    print ('retrieving sierra records')
    results = search_sierra(api_key=api_key, api_secret=api_secret, id_list=sierra_ids)
    print ('writing records')
    rows = []
    fieldnames = []
    for result in results:
        print (result)
        # row_dict = {}
        # row_keys = []
        # for key in result:
        #     if key not in fieldnames:
        #         fieldnames.append(key)
        #     if key not in row_keys:
        #         row_dict[key] = result[key]
        #         row_keys.append(key)
        #     else:
        #         row_dict[key] = str(row_dict[key]) + ' ' + str(result[key])
        #     if key == 'fixedFields':
        #         for k in result[key]:
        #             if k not in fieldnames:
        #                 fieldnames.append(k)
        #             if k not in row_keys:
        #                 row_dict[k] = [result[key][k]]
        #                 row_keys.append(k)
        #             else:
        #                 row_dict[k].append(result[key][k])
        #     if key == 'varFields':
        #         x=0
        #         for item in result[key]:
        #             try:
        #                 if item['marcTag'] not in fieldnames:
        #                     fieldnames.append(item['marcTag'])
        #                 if item['marcTag'] not in row_keys:
        #                     row_dict[item['marcTag']] = [item]
        #                     row_keys.append(item['marcTag'])
        #                 else:
        #                     row_dict[item['marcTag']].append(item)
        #             except KeyError:
        #                 pass
        #             x+=1
        #
        # rows.append(row_dict)
        # print (row_dict)
        # x += 1
    # with open('sierra_records.csv', 'w') as write_file:
    #     fieldnames = fieldnames
    #     writer = csv.DictWriter(write_file, fieldnames)
    #     writer.writeheader()
    #     for row in rows:
    #         writer.writerow(row)
    print ('done')
    # print (results)
    # for item in results:
    #     if isinstance(results[item], dict):
    #         for entry in results[item]:
    #             print (item, entry, results[item][entry])
    #     elif isinstance(results[item], list):
    #         for entry in results[item]:
    #             for key in entry:
    #                 print (item, key, entry[key])
    #     else:
    #         print (item, results[item])

    # return sierra_records


def retrieve_bounding_box():
    results = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    print ('retrieving catalog_id')
    cursor.execute(
        """SELECT DISTINCT(catalog_id)
        FROM ams_topo.ams_catalog
        WHERE ams_topo.ams_catalog.oclc = %s""",
        ('6613121',)
    )
    catalog_ids = cursor.fetchall()
    print ('retrieving file records')
    for catalog_id in catalog_ids:
        print (catalog_id[0])
        cursor.execute(
            """SELECT ST_AsGeoJSON(ams_topo.ams_footprints.geom), ams_topo.place_names.name
            FROM ams_topo.ams_footprints
            JOIN ams_topo.ams_file ON ams_topo.ams_file.gid=ams_topo.ams_footprints.gid
            JOIN ams_topo.place_lookup ON ams_topo.ams_file.file_id=ams_topo.place_lookup.file_id
            JOIN ams_topo.place_names ON ams_topo.place_lookup.place_id=ams_topo.place_names.place_id
            WHERE ams_topo.ams_file.catalog_id = %s
            """,
            (catalog_id[0],)
        )
        print ('done reading file and geom tables')
        results = cursor.fetchall()
        place_names = []
        bounding_box_dict = {}
        x = 0
        for result in results:
            coor_list = (ast.literal_eval(result[0])['coordinates'])
            for item in coor_list:
                coordinates = item[0]
                min_latitude = coordinates[0][1]
                min_longitude = coordinates[0][0]
                max_latitude = coordinates[2][1]
                max_longitude = coordinates[2][0]
                if x == 0:
                    bounding_box_dict['min_latitude'] = min_latitude
                    bounding_box_dict['min_longitude'] = min_longitude
                    bounding_box_dict['max_latitude'] = max_latitude
                    bounding_box_dict['max_longitude'] = max_longitude
                if min_latitude < bounding_box_dict['min_latitude']:
                    bounding_box_dict['min_latitude'] = min_latitude
                if min_longitude < bounding_box_dict['min_longitude']:
                    bounding_box_dict['min_longitude'] = min_longitude
                if max_latitude > bounding_box_dict['max_latitude']:
                    bounding_box_dict['max_latitude'] = max_latitude
                if max_longitude > bounding_box_dict['max_longitude']:
                    bounding_box_dict['max_longitude'] = max_longitude
            print (bounding_box_dict)
            if result[-1] not in place_names:
                place_names.append(result[-1])
            x += 1
        print (place_names)
        print (bounding_box_dict)
        print ('end of results')
    print ('done')
