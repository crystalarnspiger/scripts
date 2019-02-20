import ast
import csv
import psycopg2
import requests
import time

from lxml import html


def database_searchtest():
    page = requests.get(
        'https://guides.lib.utexas.edu/az.php?q=medical'
    )
    results = html.fromstring(page.content)
    print (results.xpath('//div[@class="s-lg-az-result-title"]'))


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


def ceadtest():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
  )
  conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    with open('relaciones_geograficas.tsv') as read_file:
        reader = csv.DictReader(read_file, delimiter='\t')
        x = 0
        key_list_dict = {}
        for row in reader:
            if x == 0:
                for key in row:
                    key_list_dict[key] = [row[key]]
                    continue
            for key in row:
                key_list_dict[key].append(row[key])
            x += 1
        for key in key_list_dict:
            duplicates = check_duplicates(key_list_dict[key])
