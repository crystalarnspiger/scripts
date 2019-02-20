import csv
import requests

import local_constants


def search_libguides(client_id, client_secret, term):
    results = []
    # print ('getting token')
    # grant_type = 'client_credentials'
    # auth=(client_id, client_secret)
    # get_token = requests.post(
    #     'https://lgapi-us.libapps.com/1.2/oauth/token',
    #     {
    #         'client_id': client_id,
    #         'client_secret': client_secret,
    #         'grant_type': grant_type
    #     },
    # ).json()
    # print ('received token')
    # print (get_token)
    # api_token = get_token['access_token']
    # print (api_token)
    # print ('token received')
    print ('calling api')
    response = requests.get(
        # 'http://lgapi-us.libapps.com/1.1/guides?site_id=7871&key=671c6c48892a1e69b7368b0031cc718e&search_terms=texas&search_match=2'
        'http://lgapi-us.libapps.com:80/1.1/guides?site_id=7871&key=671c6c48892a1e69b7368b0031cc718e&search_terms=texas&search_match=2&sort_by=relevance&expand=owner%2Cpages%2Csubjects'
        # 'http://lgapi-us.libapps.com:80/1.1/subjects?site_id=7871&key=671c6c48892a1e69b7368b0031cc718e'
        # &search_match=2
        # &expand=pages.boxes',
        # headers={
        #     # 'Content-Type': 'application/json',
        #     'Authorization': 'Bearer {0}'.format(
        #         api_token
        #     )
        # }
    )
    print ('response ************', response)
    response = response.json()
    # print ('response **************', response)
    # print (len(response))
    x = 0
    for item in response:
        results.append(item)
        # x += 1
        # if x == 10:
        #     print ('break')
        #     break

    print ('done reading api')

    return results


def call_libguides():
    client_id = local_constants.libguides_client
    client_secret = local_constants.libguides_secret
    term = 'texas'
    rows = search_libguides(client_id, client_secret, term)
    x = 0
    for row in rows:
        if x >= 5:
            break
        print (x)
        print (row)
        x += 1
        # x += len(row['pages'])
        # print (x, len(row['pages']))
        # for page in row['pages']:
        #     x += 1
        #     print (x)
        #     print (len(row['pages']))
    # print (x)
    #     print (row)
        # for key in row:
        #     if key == 'pages':
        #         print ('pages: ')
        #         pages = row[key]
        #         for page in pages:
        #             for key2 in page:
        #                 print ('{0}: {1}'.format(key2, page[key2]))
        #     else:
        #         print ('{0}: {1}'.format(key, row[key]))
    # print ('writing file')
    # with open('libguides.csv', 'w') as write_file:
    #     fieldnames = list(rows[0].keys())
    #     writer = csv.DictWriter(write_file, fieldnames)
    #     print ('writing headers')
    #     writer.writeheader()
    #     for row in rows:
    #         print ('writing row')
    #         writer.writerow(row)
    # print ('done writing file')
