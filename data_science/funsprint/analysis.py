import ast
import codecs
import csv
import numpy
import pandas
import psycopg2
import requests

from datetime import datetime, date


def check_files():
    # sierra_items = pandas.read_csv('../documents/sierra_items.csv')
    # print (sierra_items.count())
    # adabas_data = pandas.read_csv('../documents/adabas_data.csv')
    # print (adabas_data.count())
    #
    # return (sierra_items.head(), adabas_data.head())
    sierra_items = []
    adabas_items = []
    common_items = []
    with open ('../documents/sierra_items.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('sierra record', x, record['barcode'])
            sierra_items.append(record['barcode'])
            x += 1
    with open ('../documents/adabas_data.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            # if x >= 5:
            #     break
            # print (record)
            print ('adabas record', x, record['owning_unit_item_number'])
            adabas_items.append(record['owning_unit_item_number'])
            x += 1
    x = 0
    for barcode in sierra_items:
        if barcode in adabas_items:
            print ('found', barcode)
        else:
            print ('blergh', x)
        x += 1


def combine_sierra_files():
    records = []

    with open ('../documents/sierra_data.csv') as read_file:
        print ('file open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            records.append(record)

    with open ('../documents/sierra_data2.csv') as read_file:
        print ('file 2 open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            records.append(record)

    with open ('../documents/sierra_data7.csv') as read_file:
        print ('file 7 open')
        print ('reading')
        reader = csv.DictReader(read_file, delimiter=',')
        x = 0
        for record in reader:
            records.append(record)

    with open ('../documents/sierra_lsf_total.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        print ('writing rows')
        for record in records:
            writer.writerow(record)

    print ('done')


def call_number_replace(call_number):
    call_number = str(call_number)
    if call_number.startswith('A'):
        return 'General Works'
    if call_number.startswith('B') or call_number.startswith('1') or call_number.startswith('2'):
        return 'Philosophy/Psycology/Religion'
    if call_number.startswith('C'):
        return 'Auxiliary Sciences of History'
    if call_number.startswith('D'):
        return 'World History'
    if call_number.startswith('E'):
        return 'History of the Americas'
    if call_number.startswith('F'):
        return 'History of the Americas'
    if call_number.startswith('G'):
        return 'Geography/Anthropology/Recreation'
    if call_number.startswith('H') or call_number.startswith('3'):
        return 'Social Sciences'
    if call_number.startswith('J'):
        return 'Political Science'
    if call_number.startswith('K'):
        return 'Law'
    if call_number.startswith('L'):
        return 'Education'
    if call_number.startswith('M'):
        return 'Music and Books on Music'
    if call_number.startswith('N'):
        return 'Fine Arts'
    if call_number.startswith('P') or call_number.startswith('4') or call_number.startswith('8'):
        return 'Language and Literature'
    if call_number.startswith('Q') or call_number.startswith('5'):
        return 'Science'
    if call_number.startswith('R'):
        return 'Medicine'
    if call_number.startswith('S'):
        return 'Agriculture'
    if call_number.startswith('T') or call_number.startswith('6'):
        return 'Technology'
    if call_number.startswith('U'):
        return 'Military Science'
    if call_number.startswith('V'):
        return 'Naval Science'
    if call_number.startswith('Z'):
        return 'Bibliography/Library Science/Information Resources'
    if call_number.startswith('0'):
        return 'Computer Science/Information/General Works'
    if call_number.startswith('7'):
        return 'Arts & Recreation'
    if call_number.startswith('9'):
        return 'History & Geography'


def date_added_vs_last_activity(df):
    try:
        date_added = datetime.strptime(df['date_added_to_storage'], '%m/%d/%y')
        last_activity = datetime.strptime(df['last_activity_date'], '%m/%d/%y')

        return int(str(last_activity - date_added).split(' ')[0])
    except (ValueError, TypeError):

        return numpy.nan


def date_added_vs_today(df):
    try:
        date_added = datetime.strptime(df['date_added_to_storage'], '%m/%d/%y')
        today = datetime.today()

        return int(str(today - date_added).split(' ')[0])
    except (ValueError, TypeError):

        return numpy.nan


def last_activity_vs_today(df):
    try:
        last_activity = datetime.strptime(df['last_activity_date'], '%m/%d/%y')
        today = datetime.today()

        return int(str(today - last_activity).split(' ')[0])
    except (ValueError, TypeError):

        return numpy.nan


def created_vs_date_added(df):
    try:
        date_added = datetime.strptime(df['date_added_to_storage'], '%m/%d/%y')
        created = df['createdDate'].split('T')[0]
        created = datetime.strptime(created, '%Y-%m-%d')

        return int(str(date_added - created).split(' ')[0])
    except (ValueError, TypeError):

        return numpy.nan


def created_vs_today(df):
    try:
        created = df['createdDate'].split('T')[0]
        created = datetime.strptime(created, '%Y-%m-%d')
        today = datetime.today()

        return int(str(today - created).split(' ')[0])
    except (ValueError, TypeError):

        return numpy.nan


def time_vs_retrieval(df):
    try:
        time_diff = int(str(df['added_vs_today']).split(' ')[0])/365.25
        calc = df['total_retrievals']/time_diff

        return calc
    except (ValueError, TypeError):

        return numpy.nan


def remove_decimal(x):
    try:
        barcode = '0' + str(x).split('.')[0]

        return barcode
    except (ValueError, TypeError):

        return numpy.nan


def adabas_analize():
    print ('opening file')
    adabas_data = pandas.read_csv('../documents/adabas_data.csv')
    print ('dropping columns')
    adabas_data = adabas_data.drop(
            [
                'description',
                'desc_keyword1',
                'desc_keyword2',
                'desc_keyword3',
                'desc_keyword4',
                'desc_keyword5',
                'box_vol_ser_nbr',
                'non_gl_sw',
                'discard_date',
                'requesting_unit_code',
                'requestor_name',
                'requestor_id',
                'reshelving_sw',
                'delete_sw',
                'emergency_retrieval_sw',
                'call_number',
                'pickup_loc',
                'resouce_in_common'

            ],
            axis=1
        )
    print ('calculating added_vs_last_activity')
    adabas_data['added_vs_last_activity'] = adabas_data.apply(lambda row: date_added_vs_last_activity(row), axis=1)
    print ('calculating added_vs_today')
    adabas_data['added_vs_today'] = adabas_data.apply(lambda row: date_added_vs_today(row), axis=1)
    print ('calculating last_activity_vs_today')
    adabas_data['last_activity_vs_today'] = adabas_data.apply(lambda row: last_activity_vs_today(row), axis=1)
    print ('calculating retrievals_per_year')
    adabas_data['retrievals_per_year'] = adabas_data.apply(lambda row: time_vs_retrieval(row), axis=1)

    return (adabas_data)


def sierra_adabas_analize():
    print ('importing sierra')
    sierra_items = pandas.read_csv('../documents/sierra_items20180521.csv', dtype='str')
    sierra_items['subject'] = sierra_items['callNumber']
    sierra_items['subject'] = sierra_items['subject'].map(lambda x: call_number_replace(x))
    print (sierra_items.head())

    print ('analizing adabas')
    adabas_data = adabas_analize()
    print (adabas_data.count())

    print ('merging data sets')
    merged = pandas.merge(sierra_items, adabas_data, how='inner', left_on='barcode', right_on='owning_unit_item_number')
    print (merged.count())

    print ('calculating created_vs_date_added')
    merged['created_vs_date_added'] = merged.apply(lambda row: created_vs_date_added(row), axis=1)
    print ('calculating created_vs_today')
    merged['created_vs_today'] = merged.apply(lambda row: created_vs_today(row), axis=1)

    print ('writing to file')
    merged.to_csv('../documents/merged.csv')

    return (merged.head())


def count_merged():
    print ('importing merged')
    merged = pandas.read_csv('../documents/merged.csv', dtype='str')

    print (merged.count())

    return (merged.head())
