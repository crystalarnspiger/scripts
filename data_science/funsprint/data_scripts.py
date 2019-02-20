import ast
import codecs
import csv
import numpy
import pandas
import psycopg2
import requests

import local_constants



HEADERS = [
    'id',
    'updatedDate',
    'createdDate',
    'deleted',
    'bibIds',
    'location',
    'status',
    'barcode',
    'callNumber',
    'itemType',
    'totalCheckouts',
    'totalRenewals'
]


def read_sierra():
    # record_numbers = []

    # with open ('../documents/adabas_data.csv') as read_file:
    #     print ('file open')
    #     print ('reading')
    #     reader = csv.DictReader(read_file, delimiter=',')
    #     x = 0
    #     locations = []
    #     for record in reader:
    #         if record['sierra_loc'] not in locations:
    #             locations.append(record['sierra_loc'])
    #     print (locations)
            # print ('reading adabas record', x)
            # record_numbers.append(record['inventory_number'])
            # if x > 26127:
            # five_remainder = x % 5
            # seven_remainder = x % 7
            # if seven_remainder == 0 and five_remainder != 0:
            #     print ('adabas record', x)
            #     record_numbers.append(record['inventory_number'])
            # x += 1

    with open ('../documents/sierra_items20180521.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = HEADERS
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()

        print ('retrieving records')
        api_key = local_constants.sierra_api_key
        api_secret = local_constants.sierra_api_secret

        print ('getting token')
        get_token = requests.post(
            'https://catalog2.lib.utexas.edu/iii/sierra-api/v5/token',
            auth=(api_key, api_secret)
        ).json()
        api_token = get_token['access_token']
        print ('token received')
        x = 0
        y = 0
        z = 0
        a = 0
        offset = 0
        limit = 1000
        keep_reading = True
        while keep_reading:
            result = requests.get(
                'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v5/items/?limit={0}&offset={1}&fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2CbibIds%2Clocation%2Cstatus%2Cbarcode%2CcallNumber%2CitemType%2CfixedFields%2CvarFields&locations=SFPC'.format(limit, offset),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer {0}'.format(
                        api_token
                    )
                }
            ).json()
            try:
                entries = result['entries']
                total = result['total']
                offset += limit
                if total < limit:
                    keep_reading = False
                for entry in entries:
                    print ('reading', x)
                    print ('token counter', y)
                    print ('records passed', z)
                    try:
                        sierra_record = {
                            'id': entry['id'],
                            'updatedDate': '',
                            'createdDate': '',
                            'deleted': '',
                            'bibIds': '',
                            'location': '',
                            'status': '',
                            'barcode': '',
                            'callNumber': '',
                            'itemType': '',
                            'totalCheckouts': '',
                            'totalRenewals': ''
                        }

                        try:
                            sierra_record['updatedDate'] = entry['updatedDate']
                        except KeyError:
                            pass
                        try:
                            sierra_record['createdDate'] = entry['createdDate']
                            print (sierra_record['createdDate'])
                        except KeyError:
                            pass
                        try:
                            sierra_record['deleted'] = entry['deleted']
                        except KeyError:
                            pass
                        try:
                            sierra_record['bibIds'] = entry['bibIds']
                        except KeyError:
                            pass
                        try:
                            sierra_record['location'] = entry['location']['name']
                        except KeyError:
                            pass
                        try:
                            sierra_record['status'] = entry['status']['display']
                        except KeyError:
                            pass
                        try:
                            sierra_record['barcode'] = entry['barcode']
                        except KeyError:
                            pass
                        try:
                            sierra_record['callNumber'] = entry['callNumber']
                        except KeyError:
                            pass
                        try:
                            sierra_record['itemType'] = entry['itemType']
                        except KeyError:
                            pass
                        try:
                            sierra_record['totalCheckouts'] = entry['fixedFields']['76']['value']
                        except KeyError:
                            pass
                        try:
                            sierra_record['totalRenewals'] = entry['fixedFields']['77']['value']
                        except KeyError:
                            pass

                        print ('writing record', a)
                        writer.writerow(sierra_record)
                        a += 1
                        x += 1
                        y += 1

                    except KeyError:
                        z += 1
                        x += 1
                        y += 1
                        pass

                if y >= 100:
                    y = 0
                    print ('getting new token')
                    get_token = requests.post(
                        'https://catalog2.lib.utexas.edu/iii/sierra-api/v5/token',
                        auth=(api_key, api_secret)
                    ).json()
                    api_token = get_token['access_token']
                    print ('new token received')
            except Exception as exception:
                print (result)
                print (exception)

        print ('done')


def read_adabas_file():

    with open ('../documents/adabas_data.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            if x >= 10:
                break
            print (record['inventory_number'])
            # print (record['sierra_loc'])
            # print (record)
            x += 1


def check_files():
    # sierra_data = pandas.read_csv('../documents/sierra_data_total.csv')
    # print (sierra_data.count())
    #
    # return (sierra_data.head(), adabas_data.head())

    # with open ('../documents/TXNWBDST.S01.SYSUT2.D18123.T1315532.csv', 'r') as read_file:
    #     print ('file open')
    #     print ('reading')
    #     reader = csv.DictReader(read_file, delimiter=',')
    #     x = 0
    #     for record in reader:
    #         x += 1

    with open ('../documents/TXNWBDST.S01.SYSUT2.D18123.T1315532.csv', 'r') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.reader((line.replace('\0','') for line in read_file))
        x = 0
        with open('adabas_temp.csv', 'w') as write_file:
            writer = csv.writer(write_file, delimiter=',')
            try:
                for record in reader:
                    print ('writing row', x)
                    writer.writerow(record)
                    if len(record) > 31:
                        print (len(record))
                    x += 1
                    print (x)
            except UnicodeError:
                print ('done')

    # adabas_data = pandas.read_csv('../documents/TXNWBDST.S01.SYSUT2.D18123.T1315532.csv')
    # print (adabas_data.count())
    #
    # return adabas_data.head()


def fix_adabas():
    adabas_records = []

    with open ('../documents/adabas_temp.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        y = 0
        for record in reader:
            x += 1
            print ('parsing record', x)
            try:
                new_record = {
                    'inventory_number': record['INV NBR'],
                    'owning_unit_item_number': record['OWNING ID'],
                    'inventory_container_number': record['CONTAINER'],
                    'box_or_tray': record['B'],
                    'description': record['DESCRIPTION'],
                    'desc_keyword1': record['KEYWORD1'],
                    'desc_keyword2': record['KEYWORD2'],
                    'desc_keyword3': record['KEYWORD3'],
                    'desc_keyword4': record['KEYWORD4'],
                    'desc_keyword5': record['KEYWORD5'],
                    'box_vol_ser_nbr': record['BVS-NBR'],
                    'owning_unit_code': record['OWNU'],
                    'non_gl_sw': record['G'],
                    'restricted_use_sw': record['R'],
                    'date_added_to_storage': record['ADDED'],
                    'last_activity_date': record['LACTIV'],
                    'discard_date': record['DSCRD'],
                    'last_activity_code': record['C'],
                    'requesting_unit_code': record['REQU'],
                    'requestor_name': record['REQUESTOR NAME'],
                    'requestor_id': record['REQ ID'],
                    'reshelving_sw': record['S'],
                    'total_retrievals': record['TOTRET'],
                    'total_emergency_retrievals': record['TOTER'],
                    'delete_sw': record['D'],
                    'emergency_retrieval_sw': record['E'],
                    'sierra_loc': record['SILOC'],
                    'call_number': record['CALL NUMBER'],
                    'pickup_loc': record['PCKUP'],
                    'digitize': record['Z'],
                    'resource_in_common': record['M'],
                }
                adabas_records.append(new_record)
            except Exception:
                y += 1
                print ('passing record', x, y)
                pass
    print ('done parsing records')
    print ('records passed', y)
    print ('total records', x)

    print ('writing new file')
    with open ('../documents/adabas_data.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = adabas_records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in adabas_records:
            writer.writerow(record)
    print ('done')



def combine_files():
    # sierra_data = pandas.read_csv('../documents/sierra_data2.csv')
    # print (sierra_data.count())
    #
    # return sierra_data.head()
    total_records = []

    with open ('../documents/sierra_data.csv', 'r') as read_file:
        print ('file 1 open')
        print ('reading file 1')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            x += 1
            print ('parsing record', x)
            parsed_record = {
                'id': record['id'],
                'updatedDate': record['id'],
                'createdDate': record['createdDate'],
                'deletedDate': record['deletedDate'],
                'deleted': record['deleted'],
                'suppressed': record['suppressed'],
                'available': record['available'],
                'lang': record['lang'],
                'title': record['title'],
                'author': record['author'],
                'materialType': record['materialType'],
                'bibLevel': record['bibLevel'],
                'publishYear': record['publishYear'],
                'catalogDate': record['catalogDate'],
                'country': record['country'],
                'agency': record['agency'],
                'call_number': record['call_number'],
            }

            if parsed_record['lang']:
                try:
                    parsed_record['lang'] = ast.literal_eval(parsed_record['lang'])['name']
                except KeyError:
                    parsed_record['lang'] = ''

            if parsed_record['materialType']:
                try:
                    parsed_record['materialType'] = ast.literal_eval(parsed_record['materialType'])['value']
                except KeyError:
                    parsed_record['materialType'] = ''

            if parsed_record['bibLevel']:
                try:
                    parsed_record['bibLevel'] = ast.literal_eval(parsed_record['bibLevel'])['value']
                except KeyError:
                    parsed_record['bibLevel'] = ''

            if parsed_record['country']:
                try:
                    parsed_record['country'] = ast.literal_eval(parsed_record['country'])['name']
                except KeyError:
                    parsed_record['country'] = ''

            total_records.append(parsed_record)

    with open ('../documents/sierra_data2.csv', 'r') as read_file:
        print ('file 1 open')
        print ('reading file 1')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            x += 1
            print ('parsing record', x)
            parsed_record = {
                'id': record['id'],
                'updatedDate': record['id'],
                'createdDate': record['createdDate'],
                'deletedDate': record['deletedDate'],
                'deleted': record['deleted'],
                'suppressed': record['suppressed'],
                'available': record['available'],
                'lang': record['lang'],
                'title': record['title'],
                'author': record['author'],
                'materialType': record['materialType'],
                'bibLevel': record['bibLevel'],
                'publishYear': record['publishYear'],
                'catalogDate': record['catalogDate'],
                'country': record['country'],
                'agency': record['agency'],
                'call_number': record['call_number'],
            }

            if parsed_record['lang']:
                try:
                    parsed_record['lang'] = ast.literal_eval(parsed_record['lang'])['name']
                except KeyError:
                    parsed_record['lang'] = ''

            if parsed_record['materialType']:
                try:
                    parsed_record['materialType'] = ast.literal_eval(parsed_record['materialType'])['value']
                except KeyError:
                    parsed_record['materialType'] = ''

            if parsed_record['bibLevel']:
                try:
                    parsed_record['bibLevel'] = ast.literal_eval(parsed_record['bibLevel'])['value']
                except KeyError:
                    parsed_record['bibLevel'] = ''

            if parsed_record['country']:
                try:
                    parsed_record['country'] = ast.literal_eval(parsed_record['country'])['name']
                except KeyError:
                    parsed_record['country'] = ''

            total_records.append(parsed_record)

    with open ('../documents/sierra_data7.csv', 'r') as read_file:
        print ('file 1 open')
        print ('reading file 1')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            x += 1
            print ('parsing record', x)
            parsed_record = {
                'id': record['id'],
                'updatedDate': record['id'],
                'createdDate': record['createdDate'],
                'deletedDate': record['deletedDate'],
                'deleted': record['deleted'],
                'suppressed': record['suppressed'],
                'available': record['available'],
                'lang': record['lang'],
                'title': record['title'],
                'author': record['author'],
                'materialType': record['materialType'],
                'bibLevel': record['bibLevel'],
                'publishYear': record['publishYear'],
                'catalogDate': record['catalogDate'],
                'country': record['country'],
                'agency': record['agency'],
                'call_number': record['call_number'],
            }

            if parsed_record['lang']:
                try:
                    parsed_record['lang'] = ast.literal_eval(parsed_record['lang'])['name']
                except KeyError:
                    parsed_record['lang'] = ''

            if parsed_record['materialType']:
                try:
                    parsed_record['materialType'] = ast.literal_eval(parsed_record['materialType'])['value']
                except KeyError:
                    parsed_record['materialType'] = ''

            if parsed_record['bibLevel']:
                try:
                    parsed_record['bibLevel'] = ast.literal_eval(parsed_record['bibLevel'])['value']
                except KeyError:
                    parsed_record['bibLevel'] = ''

            if parsed_record['country']:
                try:
                    parsed_record['country'] = ast.literal_eval(parsed_record['country'])['name']
                except KeyError:
                    parsed_record['country'] = ''

            total_records.append(parsed_record)


    with open ('../documents/sierra_lsf_total.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = HEADERS
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in total_records:
            writer.writerow(record)


def combine_sierra_items():

    all_items = {}

    with open ('../documents/sierra_items.csv', 'r') as read_file:
        print ('file sierra_items open')
        print ('reading file sierra_items')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        y = 0
        z = 0
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('parsing record', x)
            print ('existing records', y)
            print ('new records', z)
            try:
                item = all_items[record['id']]
                y += 1
            except KeyError:
                all_items[record['id']] = record
                z += 1
            x += 1

    with open ('../documents/sierra_items2.csv', 'r') as read_file:
        print ('file sierra_items2 open')
        print ('reading file sierra_items2')
        reader = csv.DictReader(read_file, delimiter=',')
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('parsing record', x)
            print ('existing records', y)
            print ('new records', z)
            try:
                item = all_items[record['id']]
                y += 1
            except KeyError:
                all_items[record['id']] = record
                z += 1
            x += 1

    with open ('../documents/sierra_items3.csv', 'r') as read_file:
        print ('file sierra_items3 open')
        print ('reading file sierra_items3')
        reader = csv.DictReader(read_file, delimiter=',')
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('parsing record', x)
            print ('existing records', y)
            print ('new records', z)
            try:
                item = all_items[record['id']]
                y += 1
            except KeyError:
                all_items[record['id']] = record
                z += 1
            x += 1

    with open ('../documents/sierra_items20180515.csv', 'r') as read_file:
        print ('file sierra_items20180515 open')
        print ('reading file sierra_items20180515')
        reader = csv.DictReader(read_file, delimiter=',')
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('parsing item', x)
            print ('existing items', y)
            print ('new items', z)
            try:
                item = all_items[record['id']]
                y += 1
            except KeyError:
                all_items[record['id']] = record
                z += 1
            x += 1

    with open ('../documents/sierra_items_all.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = HEADERS
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        print ('writing file')
        x = 0
        for key in all_items:
            print ('writing item', x)
            writer.writerow(all_items[key])
            x += 1

    print ('done')
