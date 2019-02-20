import csv
import requests
from xml.etree import ElementTree


def search_journals(term):
    results = []
    print ('calling api')
    response = requests.get(
        'http://te7fv6dm8k.openurl.xml.serialssolutions.com/openurlxml?version=1.0&rft.jtitle={}'.format(term)
    )
    print (response)
    # tree = ElementTree.fromstring(response.content)
    # print (tree)
    print (response.content)
    # print ('response **************', response)
    # print (len(response))
    # x = 0
    # for item in response:
    #     results.append(item)
    #
    # print ('done reading api')
    #
    # return results


def call_journals():
    term = 'gnomon'
    rows = search_journals(term)
