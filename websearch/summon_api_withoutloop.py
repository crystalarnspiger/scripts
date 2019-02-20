#!/usr/bin/python
# -*- coding: utf-8 -*-
import base64, hashlib, hmac, json, time
from datetime import datetime
import urllib2
from urllib import unquote_plus

import local_constants


def search_summon_v2(query_term, api_id, api_key, page_number=0, page_size=10, sort=False, doctype="xml"):

    """ Returns results from Summon API.
      ... total rip-off of code found at https://gist.github.com/lawlesst/1070641.
      :-]
      found this at http://blog.humaneguitarist.org/2014/09/04/getting-started-with-the-summon-api-and-python/
    """

    # set API host and path.
    host = "api.summon.serialssolutions.com"
    path = "/2.0.0/search"

    # sort and encode $query.
    # query = "s.q=" + query_term + "&s.fvf=" + 'ContentType,Journal+Article' + ("&s.pn=%d&s.ps=%d") %(page_number, page_size)
    query = "s.q=" + query_term + "&s.fvgf=" + 'ContentType,or,Journal+Article,Trade+Publication+Article,Newspaper+Article,Magazine+Article,Book+Review' + ("&s.pn=%d&s.ps=%d") %(page_number, page_size) + "&s.ho=True"

    if sort != False:
        query = query + "&s.sort=PublicationDate:desc"
    query_sorted = "&".join(sorted(query.split("&")))
    # print (query_sorted)
    query_encoded = unquote_plus(query_sorted)
    print (query_encoded)
    # print (query_encoded)

    # create request headers.
    date = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    id_string = "\n".join(["application/json", date, host, path, query_encoded, ''])
    digest = base64.encodestring(hmac.new(api_key, unicode(id_string), hashlib.sha1).digest())
    authorization = "Summon " + "utexas" + ";" + str(digest).rstrip('\n')
    headers = {
    "Accept":"application/json",
    "x-summon-date":date,
    "Host":"api.summon.serialssolutions.com",
    "Authorization":authorization}

    # send search to API; return results.
    url = "http://%s%s?%s" % (host, path, query)
    request = urllib2.Request(url=url, headers=headers)
    results = json.loads(urllib2.urlopen(request).read())

    return results


def callsummon():
    query_term = 'Straße'
    api_id = local_constants.summon_api_id
    api_key = local_constants.summon_api_key
    results = search_summon_v2(query_term=query_term, api_id=api_id, api_key=api_key, doctype='json')
    print (results)
    x = 0
    print (results['query'])
    for document in results['documents']:
        if x < 3:
            print (document)
        x += 1
