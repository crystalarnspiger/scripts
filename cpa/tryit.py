import json, requests, psycopg2, datetime

from bs4 import BeautifulSoup as bs

from cpa import local_constants


ALPHABET = [
    '',
    'A',
    'B',
    'C',
    'D',
    'E',
    'F',
    'G',
    'H',
    'I',
    'J',
    'K',
    'L',
    'M',
    'N',
    'O',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'U',
    'V',
    'W',
    'X',
    'Y',
    'Z',
    'stop'
]


def increment_letter(word):
    keep_going = True

    while keep_going:
        letter_list = list(word)

        try:
            alphabet_place = ALPHABET.index(letter_list[-1])
            alphabet_place += 1
            letter_list[-1] = ALPHABET[alphabet_place]
        except ValueError:
            letter_list[-1] = 'A'

        if letter_list[-1] == 'stop':
            letter_list[-1] = ''
        else:
            keep_going = False

        word = ''.join(letter_list)

        try:
            last_letter = word[-1]
        except IndexError:
            keep_going = False

    return word


def extend_key(keyword, word):
    if keyword == word:
        return ''
    keyword_list = list(keyword)
    word_list = list(word)
    keyword_list.append(word_list[len(keyword_list)])
    keyword = ''.join(keyword_list)

    return keyword


def write_the_file(data_dict):
    write_list = []
    for key in data_dict:
        write_list.append(data_dict[key])

    with open('output.csv', 'w') as write_file:
        print ('file open')
        fieldnames = write_list[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        for record in write_list:
            print (record['license_cert_number'])
            writer.writerow(record)

        return 'done'

def testcall():
    people_list = []
    key_first_name = ''
    key_last_name = ''
    keep_reading = True
    connect_string = "dbname={0} user={1} host={2} password={3}".format(
      local_constants.local_db_name,
      local_constants.local_db_user,
      local_constants.local_db_host,
      local_constants.local_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()

    y = 0
    z = 0
    c = 0
    while keep_reading:
        print ('making call', y)

        cookies = {
            '_utma': '62186238.2118684823.1534549103.1534549103.1534549103.1',
            # '_utmb': '62186238.3.10.1534446115',
            '_utmc': '62186238',
            '_utmz': '62186238.1534549103.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
            'AWSELB': 'D9F13F611CFA22C348D4AA28F08BE9999B94D9C1EB52AA3C5654C5EEDA4590C6C46DEC2A4597E51A0BD54D6CEE0F355D0DB6021C65B6DD80C8AD4BDEE472AF8CB0718B69A0',
            'CPAVERIFY_SID': '1e4b5165378fac98ede07d0b1827ca3f',
        }

        result = requests.get(
            'https://app.cpaverify.org/search_result?count=250'
            '&sort=last_name&last_sort=&desc=&limit=&individual=1'
            '&firm=&search_type=I'
            '&first_name={0}&middle_name=&last_name={1}&firm_name='
            '&nasba_id=&license_number=&jurisdiction=all&status='
            '&page=1'.format(key_first_name, key_last_name),
            cookies=cookies
        )

        print ('parsing results')
        stuff = bs(result.text, 'html.parser')
        table_data = stuff.find(class_='data_table')
        try:
            people = table_data.find_all('tr')

            print ('adding people to list')
            x = 0
            n = 0
            for person in people:
                n += 1
                if n == 1:
                    continue
                else:
                    x += 1
                    z += 1
                things = person.find_all('td')
                person_list = []
                m = 0
                for thing in things:
                    m += 1
                    if m == 1:
                        links = thing.find_all('a')
                        link_split = str(links[0]).split('\"')
                        link = link_split[1]
                        person_list.append(link)
                    else:
                        edited = thing.contents[0].strip()
                        person_list.append(edited)

                print ('adding person to database', person_list[1])
                cursor.execute(
                    """INSERT INTO public.results
                    (
                    link, last_name, first_name, middle_name, jurisdiction,
                    license_cert_number, license_cert_status, enforcement
                    )
                    VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                    )""",
                    (
                        person_list[0],
                        person_list[1],
                        person_list[2],
                        person_list[3],
                        person_list[4],
                        person_list[5],
                        person_list[6],
                        person_list[7]
                    )
                )

                conn.commit()
                c += 1

                print ('person', z, 'commit', c)

        except AttributeError:
            x = 0

        print ('setting keys')
        print (key_first_name, key_last_name)
        if key_first_name:
            if x < 250:
                key_first_name = increment_letter(key_first_name)

                if not key_first_name:
                    key_last_name = increment_letter(key_last_name)

                print (key_first_name, key_last_name)

            else:
                key_first_name = extend_key(key_first_name, person_list[2])
                if not key_first_name:
                    key_last_name = extend_key(key_last_name, person_list[1])
                if not key_last_name:
                    key_last_name = increment_letter(person_list[1])

                print (key_first_name, key_last_name)

        else:

            if x < 250:
                print (key_first_name, key_last_name)
                key_last_name = increment_letter(key_last_name)
                if not key_last_name:
                    keep_reading = False

                print (key_first_name, key_last_name)

            else:
                key_last_name = extend_key(key_last_name, person_list[1])
                if not key_last_name:
                    key_first_name = extend_key(key_first_name, person_list[2])
                    key_last_name = person_list[1]

                print (key_first_name, key_last_name)

        y += 1

    # print ('writing results to file')
    # result = write_the_file(people_list)

    return 'done'


def clean_it_up():
    people = []
    licenses = []

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
        """SELECT DISTINCT last_name, first_name, middle_name,
        jurisdiction, license_cert_number,
        license_cert_status, enforcement
        FROM public.results
        WHERE link IS NULL;"""
    )
    rows = cursor.fetchall()

    start = datetime.datetime.now()

    for row in rows:
        now = datetime.datetime.now()
        time_difference = now - start
        time_difference_in_minutes = time_difference / datetime.timedelta(minutes=1)
        print ('minutes', time_difference_in_minutes)

        print ('number', rows.index(row), 'out of', len(rows))

        last_name = row[0]
        first_name = row[1]
        middle_name = row[2]
        jurisdiction = row[3]
        license = row[4]
        license_cert_status = row[5]
        enforcement = row[6]

        print ('making call')

        cookies = {
            '_utma': '62186238.1089290716.1534352618.1534356492.1534446115.3',
            # '_utmb': '62186238.3.10.1534446115',
            '_utmc': '62186238',
            '_utmz': '62186238.1534446115.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
            'AWSELB': 'D9F13F611CFA22C348D4AA28F08BE9999B94D9C1EB8E5BD6E05B8793FFC4B60D7AC08EBED37DE2FB228A40E9D2D2A10B8F26B4355C26590F22DEEDAA3D1E6D55B4C630319F',
            'CPAVERIFY_SID': '0f662231943be1bdb4e2986de134537d',
        }

        result = requests.get(
            'https://app.cpaverify.org/search_result?count=250'
            '&sort=last_name&last_sort=&desc=&limit=&individual=1'
            '&firm=&search_type=I'
            '&first_name={0}&middle_name={1}&last_name={2}&firm_name='
            '&nasba_id=&license_number={3}&jurisdiction=all&status='
            '&page=1'.format(
                first_name,
                middle_name,
                last_name,
                license
            ),
            cookies=cookies
        )

        print ('parsing results')
        stuff = bs(result.text, 'html.parser')
        table_data = stuff.find(class_='data_table')
        try:
            people = table_data.find_all('tr')

            print ('adding people to list')

            m = 0
            for person in people:
                link = ''
                m += 1
                if m < 2:
                    continue
                elif m > 2:
                    break
                things = person.find_all('td')
                links = things[0].find_all('a')
                link_split = str(links[0]).split('\"')
                link = link_split[1]

                print ('updating person in database')
                cursor.execute(
                    """UPDATE public.results
                    SET link = %s
                    WHERE results.last_name = %s
                    AND results.first_name = %s
                    AND results.middle_name = %s
                    AND results.jurisdiction = %s
                    AND results.license_cert_number = %s
                    AND results.license_cert_status = %s
                    AND results.enforcement = %s
                    RETURNING results.license_cert_number""",
                    (
                        link,
                        last_name,
                        first_name,
                        middle_name,
                        jurisdiction,
                        license,
                        license_cert_status,
                        enforcement
                    )
                )
                license_cert_number = cursor.fetchone()[0]
                print ('person', link, first_name, middle_name, last_name, license_cert_number)

        except AttributeError as exception:
            print (exception)

        conn.commit()
