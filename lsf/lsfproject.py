import ast
import codecs
import csv
import numpy
import pandas
import psycopg2
import requests

# import local_constants



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


# def read_adabas_file():
#
#     with open ('../documents/adabas_data.csv') as read_file:
#         print ('file open')
#         print ('reading')
#         reader = csv.DictReader(read_file, delimiter=',')
#         x = 0
#         for record in reader:
#             if x >= 10:
#                 break
#             print (record['inventory_number'])
#             # print (record['sierra_loc'])
#             # print (record)
#             x += 1


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

    with open ('data.txt', 'r') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.reader((line.replace('\0','') for line in read_file))
        x = 0
        with open('lsf_adabas_temp.csv', 'w') as write_file:
            writer = csv.writer(write_file, delimiter=',')
            try:
                for record in reader:
                    print ('writing row', x)
                    writer.writerow(record)
                    if len(record) > 31:
                        print (len(record))
                    x += 1
            except UnicodeError:
                print ('done')

    # adabas_data = pandas.read_csv('../documents/TXNWBDST.S01.SYSUT2.D18123.T1315532.csv')
    # print (adabas_data.count())
    #
    # return adabas_data.head()


def fix_adabas():
    adabas_records = []

    with open ('lsf_adabas_temp.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        y = 0
        for record in reader:
            x += 1
            print ('parsing record', x)
            try:
                print (record['OWNING ID'])
                new_record = {
                    'inventory_number': record['INV NBR'],
                    'owning_unit_item_number': str(record['OWNING ID']),
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
    with open ('lsf_adabas_data.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = adabas_records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in adabas_records:
            writer.writerow(record)
    print ('done')


def read_sierra_requests():
    print ('here')
    barcodes = []
    # file_name = '../notices/lsfrequests.t{0}.auton'.format(self.datestamp)
    with open ('lsfrequests.t180828.auton', 'rb') as read_file:
        entries = read_file.readlines()

        # entries = entries.split(b'\n')

        for entry in entries:
            if b'BARCODE' in entry:
                entry = entry.split(b':')
                barcode = entry[-1].lstrip().decode()
                print ('barcode', barcode)
                barcodes.append(barcode.strip('\n'))
    print ('barcode list', barcodes)

    return barcodes


def run_adabas():
    # print ('reading adabas file')
    # read_adabas_file()
    print ('checking files')
    check_files()
    print ('fixing adabas')
    fix_adabas()
    print ('reading fixed file')
    adabas_by_barcode = {}
    adabas_barcode_list = []
    with open ('lsf_adabas_data.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        y = 0
        relevant_records = []
        for record in reader:
            print ('reading record', y)
            if record['last_activity_code'] in ['O','T','P','Q']:
                print ('appending record', x, '*****************************')
                relevant_records.append(record)
                adabas_by_barcode[record['owning_unit_item_number']] = record
                adabas_barcode_list.append(record['owning_unit_item_number'])
                x += 1

            y += 1
    print ('done reading file')
    # for record in relevant_records:
        # if record['last_activity_date'].endswith('18'):
        #     print ('box_or_tray', record['box_or_tray'])
        #     print ('requesting_unit_code', record['requesting_unit_code'])
        #     print ('requestor_id', record['requestor_id'])
        #     print ('last_activity_code', record['last_activity_code'])
        #     print ('last_activity_date', record['last_activity_date'])

    print (len(relevant_records))

    print ('writing new file')
    with open ('lsf_relevant_b.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = relevant_records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in relevant_records:
            if record['last_activity_date'].endswith('18'):
                # print ('writing row', record['inventory_number'])
                print (str(record['owning_unit_item_number']))
                writer.writerow(record)

    barcodes = read_sierra_requests()
    print (adabas_barcode_list)
    print (barcodes)

    print ('reading barcodes')
    with open ('sierra_requests.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = relevant_records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for barcode in barcodes:
            try:
                writer.writerow(adabas_by_barcode[barcode])
            except KeyError:
                pass

    print ('done running adabas')
