import ast
import csv
import datetime
import psycopg2
import requests
import time
import urllib

import local_constants


def check_duplicates(values):
    duplicates = False
    if '' in values:
        values = values.remove('')
    if values:
        deduped = set(values)
        print ('values: {0}'.format(values))
        print ('deduped: {0}'.format(deduped))
        if len(deduped) < len(values):
            print (len(deduped), len(values))
            return (True, list(deduped))
    return (False, values)


def testrun_csvcreate():
    connect_string = "dbname={0} user={1} host={2} password={3}".format(
      local_constants.local_db_name,
      local_constants.local_db_user,
      local_constants.local_db_host,
      local_constants.local_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        filemakerdata.contacts.id_contact,
        filemakerdata.contacts.name_first,
        filemakerdata.contacts.name_middle,
        filemakerdata.contacts.name_last,
        filemakerdata.contacts.birthdate,
        filemakerdata.contacts.birth_city,
        filemakerdata.contacts.birth_state,
        filemakerdata.contacts.birth_country,
        filemakerdata.interviews.interviewer_name,
        filemakerdata.interviews.location,
        filemakerdata.interviews.interview_date,
        filemakerdata.military_enlistments.branch,
        filemakerdata.collections.collection_name,
        vocesdata.metaman_dump.image_url,
        vocesdata.metaman_dump.id,
        vocesdata.metaman_dump.story_location
        FROM filemakerdata.contacts
        JOIN filemakerdata.interviews ON filemakerdata.contacts.id_contact = filemakerdata.interviews.id_contact
        JOIN filemakerdata.military_enlistments ON filemakerdata.contacts.id_contact = filemakerdata.military_enlistments.id_contact
        JOIN filemakerdata.collections ON filemakerdata.contacts.id_contact = filemakerdata.collections.id_contact
        JOIN vocesdata.metaman_dump ON filemakerdata.contacts.subject_number = vocesdata.metaman_dump.id
        WHERE filemakerdata.contacts.id_contact IN ('46516', '46621', '46642', '46913', '46917')
        """
    )
    entries = cursor.fetchall()
    records = []
    x = 0
    for entry in entries:
        print ('entry[4]', entry[4])
        subject = {
            'id_contact': entry[0],
            'name_first': entry[1],
            'name_middle': entry[2],
            'name_last': entry[3],
            'birthdate': datetime.datetime.strptime(entry[4], '%m/%d/%y'),
            'birth_city': entry[5],
            'birth_state': entry[6],
            'birth_country': entry[7],
            'interviewer_name': entry[8],
            'branch': entry[11],
            'collection_name': entry[12],
            'images': entry[13],
            'id': entry[14],
            'story_location': 'http://legacy{0}'.format(entry[15].lstrip('www').rstrip(' ')),
            'story_html': '',
            'story_linebreaks': '',
            'interview_location': entry[9],
            'interview_date': entry[10],

        }
        print ('strptime', subject['birthdate'])
        print ('strftime', subject['birthdate'].strftime('%m/%d/%Y'))
        page = requests.get(
            subject['story_location']
        )
        subject['story_html'] = page.content.decode('utf-8').replace('\\n', '').replace('\\r', '').lstrip("'b")
        # print (type(subject['story_html']))
        # print (subject['story_html'])
        # subject['story_linebreaks'] = '"{0}"'.format(str(page.content).replace('</p>', '\\n').replace('<p>', ''))
        # print (subject['id_contact'])
        records.append(subject)
        x += 1
        if x >= 5:
            break
    with open('../../Documents/voces/voces_upload_sample_20180215.csv', 'w') as write_file:
        print ('writing file')
        fieldnames = records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in records:
            print (record['id_contact'])
            writer.writerow(record)
    print ('done')


def fixid():
    connect_string = "dbname={0} user={1} host={2} password={3}".format(
      local_constants.local_db_name,
      local_constants.local_db_user,
      local_constants.local_db_host,
      local_constants.local_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id
        FROM vocesdata.metaman_dump"""
    )
    entries = cursor.fetchall()
    print ('done with select')
    for entry in entries:
        print (entry)
        original = entry[0]
        print (original)
        corrected = original.strip(' ')
        corrected = corrected.lstrip('0')
        print (corrected)
        print ('updating')
        cursor.execute(
            """UPDATE vocesdata.metaman_dump
            SET id = %s
            WHERE id = %s
            RETURNING id""",
            (corrected, original)
        )
        verify = cursor.fetchone()
        print ('updated', verify)
    print ('committing')
    conn.commit()
    print ('done')
    # with open('../../Documents/voces/filemakerraw.csv') as read_file:
    #     reader = csv.DictReader(read_file, delimiter=',')
    #     for row in reader:


def check_weird_notes():
    print ('starting')
    with open('../../Documents/voces/mybox-selected/Military_Enlistments.csv') as read_file:
        print ('reading file')
        reader = csv.reader(read_file, delimiter=',')
        records = []
        x = 0
        for row in reader:
            x += 1
            print (row)
            if x >= 5:
                break
    #         new_row = []
    #         for item in row:
    #             new_row.append(item.replace('\n','; '))
    #         print (new_row)
    #         records.append(new_row)
    #
    # with open('../../Documents/voces/mybox-selected/ContactsRewrite.csv', 'w') as write_file:
    #     print ('writing file')
    #     writer = csv.writer(write_file, delimiter=',')
    #     writer.writerows(records)

    print ('done')


def create_csv_for_upload():
    print ('starting')
    with open('../../Documents/voces/fullvocesraw.csv') as read_file:
        print ('reading file')
        reader = csv.DictReader(read_file, delimiter=',')
        records = []
        x = 0
        for row in reader:
            print (x)
            interviewer_firstname = ''
            interviewer_middlename = ''
            interviewer_lastname = ''
            interview_city = ''
            interview_state = ''
            if row['name_middle']:
                full_name = '{0} {1} {2}'.format(
                    row['name_first'],
                    row['name_middle'],
                    row['name_last']
                )
            else:
                full_name = '{0} {1}'.format(
                    row['name_first'],
                    row['name_last']
                )

            records.append({
                'metaman_id': row['id'],
                'filemaker_id_contact': row['id_contact'],
                'full_name': full_name,
                'image_path': row['image_url'],
                'subject_firstname': row['name_first'],
                'subject_middlename': row['name_middle'],
                'subject_lastname': row['name_last'],
                'subject_birthdate': row['birthdate'],
                'interviewer': row['interviewer'],
                'subject_militarybranch': row['military_branch'],
                'subject_birthcity': row['birth_city'],
                'subject_birthstate': row['birth_state'],
                'subject_birthcountry': row['birth_country'],
                'interview_city': interview_city,
                'interview_state': interview_state,
                'collection': row['collection_name']
            })
            x += 1

    with open('../../Documents/voces/voces_upload.csv', 'w') as write_file:
        print ('writing file')
        fieldnames = records[0].keys()
        writer = csv.DictWriter(write_file, fieldnames)
        writer.writeheader()
        x = 0
        for record in records:
            print (x)
            writer.writerow(record)
            x += 1
    print ('done')
