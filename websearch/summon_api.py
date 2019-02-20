import ast, base64, hashlib, hmac, json, time
from datetime import datetime
import urllib2
from urllib import unquote_plus

import local_constants


def search_summon_v2(query_term, api_id, api_key, page_number=0, page_size=50, sort=False, doctype="xml"):

    """ Returns results from Summon API.
      ... total rip-off of code found at https://gist.github.com/lawlesst/1070641.
      :-]
      found this at http://blog.humaneguitarist.org/2014/09/04/getting-started-with-the-summon-api-and-python/
    """
    start_time = time.time()
    summon_records = []
    keep_reading = True
    session_id = ''
    # set API host and path.
    print ('start reading', 'record count: ', 0, 'page_size: ', page_size, 'page count: ', 0, 'page_number: ', page_number)
    while keep_reading:
        print ('starting read', page_number, page_size)
        host = "api.summon.serialssolutions.com"
        path = "/2.0.0/search"

        # sort and encode $query.
        query = "s.q=" + query_term + "&s.fvgf=" + 'ContentType,or,Journal+Article,Trade+Publication+Article,Newspaper+Article,Magazine+Article,Book+Review' + ("&s.pn=%d&s.ps=%d") %(page_number, page_size) + "&s.ho=True"
        # query = "s.q=Title:" + query_term + "&s.fvf=" + 'ContentType,Book' + ("&s.pn=%d&s.ps=%d") %(page_number, page_size) + "&s.ho=True"
        # query = "s.q=Title:" + query_term + ("&s.pn=%d&s.ps=%d") %(page_number, page_size) + "&s.ho=True"
        # query = "s.q=Title:" + query_term + "&s.fvgf=" + 'ContentType,not,Book,eBook,Research+Guide' + ("&s.pn=%d&s.ps=%d") %(page_number, page_size) + "&s.ho=True"
        print ('query', query)
        # if sort != False:
        #     query = query + "&s.sort=PublicationDate:desc"
        query_sorted = "&".join(sorted(query.split("&")))
        # print (query_sorted)
        query_encoded = unquote_plus(query_sorted)
        # print (query_encoded)

        # create request headers.
        date = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        id_string = "\n".join(["application/json", date, host, path, query_encoded, ''])
        digest = base64.encodestring(hmac.new(api_key, unicode(id_string), hashlib.sha1).digest())
        authorization = "Summon " + "utexas" + ";" + str(digest).rstrip('\n')
        if session_id:
            print ('setting session id', session_id)
            headers = {
            "Accept":"application/json",
            "x-summon-date":date,
            "Host":"api.summon.serialssolutions.com",
            "Authorization":authorization,
            "x-summon-session-id": session_id
            }
        else:
            headers = {
            "Accept":"application/json",
            "x-summon-date":date,
            "Host":"api.summon.serialssolutions.com",
            "Authorization":authorization
            }
        # send search to API; return results.
        elapsed_time = time.time() - start_time
        print (elapsed_time)
        try:
            url = "http://%s%s?%s" % (host, path, query)
            request = urllib2.Request(url=url, headers=headers)
            results = json.loads(urllib2.urlopen(request).read())
        except urllib2.HTTPError:
            headers = {
            "Accept":"application/json",
            "x-summon-date":date,
            "Host":"api.summon.serialssolutions.com",
            "Authorization":authorization}
            url = "http://%s%s?%s" % (host, path, query)
            request = urllib2.Request(url=url, headers=headers)
            results = json.loads(urllib2.urlopen(request).read())
        summon_records.append(results['documents'])
        if results['pageCount'] == page_number or page_number >= 20:
            print ('done reading', 'record count: ', results['recordCount'], 'page_size: ', page_size, 'page count: ', results['pageCount'], 'page_number: ', page_number)
            keep_reading = False
        elif page_number < results['pageCount']:
            page_number += 1
            session_id = results['sessionId']
            print ('looping again', 'record count: ', results['recordCount'], 'page_size: ', page_size, 'page count: ', results['pageCount'], 'page_number: ', page_number)

    return summon_records


def callsummon():
    query_term = 'texas'
    api_id = local_constants.summon_api_id
    api_key = local_constants.summon_api_key
    results = search_summon_v2(query_term=query_term, api_id=api_id, api_key=api_key, doctype='json')
    content_types = []
    x = 0
    for result_list in results:
        for result in result_list:
            if x < 3:
                print (result)
            for content_type in result['ContentType']:
                if content_type not in content_types:
                    content_types.append(content_type)
            if 'Book' in result['ContentType'] and 'eBook' not in result['ContentType']:
                print (result['ContentType'])
            if result['inHoldings'] is False:
                print ('*************', result['inHoldings'])
                if x < 1:
                    print (result)
            x += 1

    print (content_types)
