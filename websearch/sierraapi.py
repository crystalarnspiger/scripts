# import base64, hashlib, hmac
# from datetime import datetime
# import urllib2
# from urllib import unquote_plus
# import zlib
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
        """SELECT DISTINCT(oclc)
        FROM ams_topo.ams_catalog"""
    )
    oclc_numbers = cursor.fetchall()
    for oclc in oclc_numbers:
        results.append(oclc[0])
    print ('done reading table')

    return results


def search_sierra(api_key, api_secret):
    records = []
    print ('getting token')
    get_token = requests.post(
        'https://catalog2.lib.utexas.edu/iii/sierra-api/v4/token',
        auth=(api_key, api_secret)
    ).json()
    api_token = get_token['access_token']
    print ('token received')
    keep_reading = True
    offset = 0
    while keep_reading:
        print ('reading first criteria', offset)
        results = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/search?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields&index=author&text=United%20States.%20Army%20Map%20Service',
            {'offset': offset},
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        records.append(results['entries'])
        if results['total'] - offset <= results['count']:
            print ('done reading first criteria')
            keep_reading = False
        elif offset < results['total']:
            print ('looping again', results['total'], results['count'])
            offset += results['count']
    keep_reading = True
    offset = 0
    while keep_reading:
        print ('reading second criteria', offset)
        results = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/search?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields&index=author&text=Great%20Britain.%20War%20Office.%20General%20Staff.%20Geographical%20Section.',
            {'offset': offset},
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        records.append(results['entries'])
        if results['total'] - offset <= results['count']:
            print ('done reading first criteria')
            keep_reading = False
        elif offset < results['total']:
            print ('looping again', results['total'], results['count'])
            offset += results['count']
    keep_reading = True
    offset = 0
    while keep_reading:
        print ('reading third criteria', offset)
        results = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/search?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields&index=author&text=United%20States.%20War%20Department.%20General%20Staff.',
            {'offset': offset},
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        records.append(results['entries'])
        if results['total'] - offset <= results['count']:
            print ('done reading first criteria')
            keep_reading = False
        elif offset < results['total']:
            print ('looping again', results['total'], results['count'])
            offset += results['count']

    print ('done reading api')

    return records


def sierra_test():
    api_key = '6duSUiHpeJvGj0v1pLfc9tcQPRSi'
    api_secret = 'RDr6^LKj4p'
    print ('getting token')
    get_token = requests.post(
        'https://catalog2.lib.utexas.edu/iii/sierra-api/v5/token',
        auth=(api_key, api_secret)
    ).json()
    api_token = get_token['access_token']
    print ('token received')
    keep_reading = True
    offset = 0
    records = []
    relevance = []
    while keep_reading:
        print ('reading', offset)
        results = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v5/bibs/search?limit=50&fields=title%2Cauthor%2CpublishYear%2CupdatedDate%2CcreatedDate%2CcatalogDate&index=Title&text=texas&SORT=DRA',
            {'offset': offset},
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        records.append(results['entries'])# entries = results['entries']
        entries = results['entries']
        print ('length of entries', len(entries))
        for entry in entries:
            # if 'wildflowers' in entry['bib']['title']:
                # print (entry)
            if entry['relevance'] not in relevance:
                relevance.append(entry['relevance'])
                print (entry['relevance'])
            print (
                entry['relevance'],
                entry['bib']['title'][0:4],
                entry['bib']['publishYear'],
                entry['bib']['author'][0:4],
                entry['bib']['id'],
                entry['bib']['updatedDate'],
                entry['bib']['createdDate'],
                entry['bib']['catalogDate']
            )
        if results['total'] - offset <= results['count']:
            print ('done reading first criteria')
            keep_reading = False
        elif offset < results['total']:
            print ('looping again', results['total'], results['count'])
            offset += results['count']
    # results = requests.get(
    #     'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v3/bibs/search?fields=title%2Cauthor%2CpublishYear&index=title&text=texas&SORT=DRA:DA',
    #     headers={
    #         'Content-Type': 'application/json',
    #         'Authorization': 'Bearer {0}'.format(
    #             api_token
    #         )
    #     }
    # ).json()
    # results = requests.post(
    #     'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v5/bibs/query?offset=0&limit=3',
    #     {
    #         "target": {
    #             "record": {"type": "bib"},
    #             "field": {"tag": "t"}
    #         },
    #         "expr": {
    #             "op": "equals",
    #             "operands": ["moby dick"]
    #         }
    #     },
    #     headers={
    #         'Content-Type': 'application/json',
    #         'Authorization': 'Bearer {0}'.format(
    #             api_token
    #         )
    #     }
    # ).json()
    # entries = results['entries']
    entries = records
    print (len(entries))
    print (relevance)
    x = 0
    for entry in entries:
        for item in entry:
            if x < 10:
                print (item)
            x += 1
    #     if 'wildflowers' in entry['bib']['title']:
    #         print (entry)
        # answer = requests.get(
        #     entry['link'],
        #     headers={
        #         'Content-Type': 'application/json',
        #         'Authorization': 'Bearer {0}'.format(
        #             api_token
        #         )
        #     }
        # ).json()
        # print (answer)


def callsierra():
    sierra_records = []
    query_list = read_table()
    api_key = local_constants.sierra_api_key
    api_secret = local_constants.sierra_api_secret
    results = search_sierra(api_key=api_key, api_secret=api_secret)
    x = 0
    found_oclc = []
    print ('matching results to oclc list')
    for result in results:
        for item in result:
            for entry in item['bib']['varFields']:
                try:
                    if entry['marcTag'] == '001':
                        if entry['content'] in query_list:
                            found_oclc.append(entry['content'])
                            sierra_records.append(item)
                            x += 1
                except KeyError:
                    pass
    print (x, len(query_list))
    for item in query_list:
        if item not in found_oclc:
            print ('not found', item)
    print ('done retrieving records')
    print ('connecting to database for id insert/update')
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection for insert/update')
    cursor = conn.cursor()
    for item in sierra_records:
        for entry in item['bib']['varFields']:
            try:
                if entry['marcTag'] == '001':
                    try:
                        cursor.execute(
                            """UPDATE ams_topo.ams_catalog
                            SET sierra_id = (%s)
                            WHERE (oclc = %s)
                            RETURNING catalog_id""",
                            (item['bib']['id'], entry['content'],)
                        )
                        print ('updated record')
                        print ('catalog_id:', cursor.fetchone()[0], 'sierra_id:', item['bib']['id'], 'oclc:', entry['content'] )
                    except TypeError:
                        cursor.execute(
                            """INSERT INTO ams_topo.ams_catalog
                            (oclc, sierra_id) VALUES (%s, %s) RETURNING catalog_id""",
                            (entry['content'], item['bib']['id'],)
                        )
                        print ('inserted record')
                        print ('catalog_id:', cursor.fetchone()[0], 'sierra_id:', item['bib']['id'], 'oclc:', entry['content'] )
            except KeyError:
                pass
    print ('committing changes')
    conn.commit()
    print ('done updating records')

    # return sierra_records
