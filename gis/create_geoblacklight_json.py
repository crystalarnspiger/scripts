import ast
import json
import psycopg2
import requests
import local_constants


geoblacklight_json = {
  "geoblacklight_version": "1.0",
  "dc_identifier_s": "",
  "layer_slug_s": "",
  "dc_title_s": "",
  "solr_geom": "",
  "dct_provenance_s": "",
  "dc_rights_s": "",
  "dc_description_s": "",
  "layer_geom_type_s": "",
  "dct_references_s": "{\"http://www.opengis.net/def/serviceType/ogc/wms\":\"\"}",
  "layer_id_s": "",
  "dc_creator_sm": [],
  "dct_isPartOf_sm": "",
  "dct_temporal_sm": [],
  "dct_issued_s": "",
  "dc_language_s": "",
  "dct_spatial_sm": [],
  "dc_publisher_s": "",
  "dc_subject_sm": [],
  "dc_type_s": ""
}
# solr_geom = (west, east, north, west)


def add_geomtype_to_ams_sheet():
    full_records = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        ams_topo.ams_sheets.sheet_id
        FROM ams_topo.ams_sheets"""
    )
    print ('ams_sheets records retrieved')
    file_records = cursor.fetchall()
    print ('processing records')
    for record in file_records:
        print (record)
        cursor.execute(
            """UPDATE ams_topo.ams_sheets
            SET geom_type = (%s)
            WHERE (sheet_id = %s)
            RETURNING sheet_id""",
            ('Paper Map', record[0])
        )
        print ('sheet_id: ', cursor.fetchone()[0], 'updated')
    print ('committing changes')
    conn.commit()
    print ('done with update')


def add_data_to_ams_sheet():
    full_records = []
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        ams_topo.ams_sheets.sheet_id,
        ams_topo.ams_sheets.sheet_name,
        ams_topo.ams_set.sierra_id,
        ams_topo.ams_set.oclc
        FROM ams_topo.ams_sheets
        JOIN ams_topo.ams_set ON ams_topo.ams_sheets.set_id = ams_topo.ams_set.set_id"""
    )
    print ('ams_sheets records retrieved')
    file_records = cursor.fetchall()
    api_key = local_constants.sierra_api_key
    api_secret = local_constants.sierra_api_secret
    print ('retrieving sierra records')
    print ('getting token')
    get_token = requests.post(
        'https://catalog2.lib.utexas.edu/iii/sierra-api/v4/token',
        auth=(api_key, api_secret)
    ).json()
    api_token = get_token['access_token']
    print ('token received')
    print ('calling api with ids')
    for record in sheet_records:
        sheet_id = record[0]
        sheet_name = record[1]
        sierra_id = record[2]
        oclc = record[3]
        print ('retrieving record', sierra_id)
        response = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/{0}?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields'.format(sierra_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        full_records.append(
            {
                'sheet_id': sheet_id,
                'sheet_name': sheet_name,
                'sierra_id': sierra_id,
                'oclc': oclc,
                'title': response['title']
            }
        )
    print ('done reading api')
    print ('processing records')
    for record in full_records:
        if not record['oclc']:
            oclc = '{0}{1}'.format(record['sierra_id'], record['sheet_id'])
        else:
            oclc = record['oclc']
        title = record['title'].split('[')
        title = title[0].replace(' ', '-').replace(':', '').replace(',', '').replace('.', '')
        '-'.join([title, sheet_name])
        identifier_url = 'lib.utexas.edu/{0}/sheet{1}'.format(oclc, record['sheet_id'])
        layer_slug = '{0}-{1}-{2}'.format(
            title,
            oclc,
            record['sheet_id']
        )
        material_type = 'Mixed'
        sheet_format = 'JPEG'
        cursor.execute(
            """UPDATE ams_topo.ams_sheets
            SET
            identifier_url = (%s),
            layer_slug = (%s),
            material_type = (%s),
            file_format = (%s)
            WHERE (sheet_id = %s)
            RETURNING sheet_id""",
            (
                identifier_url,
                layer_slug,
                material_type,
                file_format,
                record['sheet_id']
            )
        )
        print ('sheet_id: ', cursor.fetchone()[0], 'updated')
    print ('committing changes')
    conn.commit()
    print ('done with update')


def retrieve_bounding_box_for_set(set_id, cursor):
    bounding_box_dict = {}
    print ('reading tables')
    cursor.execute(
        """SELECT ST_AsGeoJSON(ams_topo.ams_footprints.geom)
        FROM ams_topo.ams_footprints
        JOIN ams_topo.ams_sheets ON ams_topo.ams_sheets.gid=ams_topo.ams_footprints.gid
        WHERE ams_topo.ams_sheets.set_id = %s
        """,
        (set_id,)
    )
    print ('done reading sheet and geom tables')
    results = cursor.fetchall()
    x = 0
    print ('processing results')
    for result in results:
        coor_list = (ast.literal_eval(result[0])['coordinates'])
        for item in coor_list:
            coordinates = item[0]
            south = coordinates[0][1]
            west = coordinates[0][0]
            north = coordinates[2][1]
            east = coordinates[2][0]
            if x == 0:
                bounding_box_dict['south'] = south
                bounding_box_dict['west'] = west
                bounding_box_dict['north'] = north
                bounding_box_dict['east'] = east
            if south < bounding_box_dict['south']:
                bounding_box_dict['south'] = south
            if west < bounding_box_dict['west']:
                bounding_box_dict['west'] = west
            if north > bounding_box_dict['north']:
                bounding_box_dict['north'] = north
            if east > bounding_box_dict['east']:
                bounding_box_dict['east'] = east
        x += 1
    print (bounding_box_dict)
    return bounding_box_dict


def retrieve_place_names_for_set(set_id, cursor):
    place_names = []
    print ('reading tables')
    cursor.execute(
        """SELECT ams_topo.place_names.name
        FROM ams_topo.place_names
        JOIN ams_topo.place_lookup ON ams_topo.place_names.place_id=ams_topo.place_lookup.place_id
        JOIN ams_topo.ams_sheets ON ams_topo.place_lookup.sheet_id=ams_topo.ams_sheets.sheet_id
        WHERE ams_topo.ams_sheets.set_id = %s
        """,
        (set_id,)
    )
    print ('done reading sheet and placename tables')
    results = cursor.fetchall()
    print ('processing results')
    for result in results:
        if result[0] not in place_names:
            try:
                name = result[0].encode(encoding='utf-8').decode('ascii')
                place_names.append(name)
            except UnicodeDecodeError:
                pass

    return place_names


def create_set_json():
    print ('connecting to database')
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    print ('successful connection to database')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        set_id,
        sierra_id,
        oclc,
        wms_url,
        holding_institution,
        rights,
        set_layer_id,
        set_identifier_url,
        set_layer_slug,
        set_material_type,
        set_geom_type,
        collection
        FROM ams_topo.ams_set"""
    )
    set_records = cursor.fetchall()
    x = 0
    json_list = []
    for set_record in set_records:
        print (set_record)
        set_id = set_record[0]
        sierra_id = set_record[1]
        oclc = set_record[2]
        wms_url = set_record[3]
        holding_institution = set_record[4]
        rights = set_record[5]
        set_layer_id = set_record[6]
        set_identifier_url = set_record[7]
        set_layer_slug = set_record[8]
        set_material_type = set_record[9]
        set_geom_type = set_record[10]
        collection = set_record[11]
        print ('retrieving bounding box')
        bounding_box_dict = retrieve_bounding_box_for_set(set_id, cursor)
        print ('retrieving place names')
        place_names = retrieve_place_names_for_set(set_id, cursor)
        print ('retrieving api data')
        print ('getting token')
        get_token = requests.post(
            'https://catalog2.lib.utexas.edu/iii/sierra-api/v4/token',
            auth=('6duSUiHpeJvGj0v1pLfc9tcQPRSi', 'RDr6^LKj4p')
        ).json()
        api_token = get_token['access_token']
        print ('token received')
        print ('calling api with id', sierra_id)
        sierra_record = requests.get(
            'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/{0}?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields'.format(sierra_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {0}'.format(
                    api_token
                )
            }
        ).json()
        title = sierra_record['title']
        author = [sierra_record['author']]
        try:
            publish_year = str(sierra_record['publishYear'])
        except KeyError:
            publish_year = ''
        language = sierra_record['lang']['name']
        description = []
        subjects = []
        publisher = []
        print ('processing varfields')
        for varfield in sierra_record['varFields']:
            try:
                marc_tag = varfield['marcTag']
            except KeyError:
                continue
            if marc_tag == '500':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in description:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            description.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
            if marc_tag == '255':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in description:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            description.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
            if marc_tag == '300':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in description:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            description.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
            if marc_tag == '507':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in description:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            description.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
            if marc_tag == '246':
                for subfield in varfield['subfields']:
                    try:
                        blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                        description.append(blurb.replace('\"', ''))
                    except UnicodeDecodeError:
                        pass
            if marc_tag == '264':
                for subfield in varfield['subfields']:
                    try:
                        blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                        publisher.append(blurb.replace('\"', ''))
                    except UnicodeDecodeError:
                        pass
            if marc_tag == '651':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in subjects:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            subjects.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
            if marc_tag == '650':
                for subfield in varfield['subfields']:
                    if subfield['content'] not in subjects:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            subjects.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
        print ('creating dictionary')
        dct_references = '{0}{1}{2}{3}:{4}{5}{6}{7}'.format(
            '{',
            '\"',
            'http://www.opengis.net/def/serviceType/ogc/wms',
            '\"',
            '\"',
            wms_url,
            '\"',
            '}'
        )
        set_dict= {
            "geoblacklight_version": "1.0",
            "dc_identifier_s": set_identifier_url,
            "layer_slug_s": set_layer_slug,
            "dc_title_s": title,
            "solr_geom": "ENVELOPE({0}, {1}, {2}, {3})".format(
            bounding_box_dict['west'],
            bounding_box_dict['east'],
            bounding_box_dict['north'],
            bounding_box_dict['south']
            ),
            "dct_provenance_s": holding_institution,
            "dc_rights_s": rights,
        }
        if description:
            set_dict["dc_description_s"] = " ".join(description)
        if set_geom_type:
            set_dict["layer_geom_type_s"] =  set_geom_type
        if dct_references:
            set_dict["dct_references_s"] =  dct_references
        if set_layer_id:
            set_dict["layer_id_s"] =  set_layer_id
        if author:
            set_dict["dc_creator_sm"] =  author
        if collection:
            set_dict["dct_isPartOf_sm"] =  collection
        if language:
            set_dict["dc_language_s"] =  language
        if place_names:
            set_dict["dct_spatial_sm"] =  place_names
        if publisher:
            set_dict["dc_publisher_s"] =  " ".join(publisher)
        if subjects:
            set_dict["dc_subject_sm"] =  subjects
        if set_material_type:
            set_dict["dc_type_s"] =  set_material_type
        if publish_year:
            set_dict["dct_temporal_sm"] = [publish_year]
            set_dict["dct_issued_s"] = publish_year
        print ('appending json list')
        json_list.append(set_dict)

    outfile_name = 'ams_topo.json'.format()
    with open(outfile_name, 'w') as outfile:
        json.dump(json_list, outfile)


def create_sheet_json():
    connect_string = "dbname='gis' user={0} host={1} password={2}".format(
      local_constants.gis_db_user,
      local_constants.gis_db_host,
      local_constants.gis_db_password
    )
    conn = psycopg2.connect(connect_string)
    # print ('successful connection for read')
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        ams_topo.ams_set.set_id
        FROM ams_topo.ams_set"""
    )
    set_ids = cursor.fetchall()
    for set_id in set_ids:
        json_list =[]
        # print ('processing sheets for set_id', set_id[0])
        cursor.execute(
            """SELECT
            ams_topo.ams_sheets.sheet_id,
            ams_topo.ams_sheets.identifier_url,
            ams_topo.ams_sheets.layer_slug,
            ams_topo.ams_sheets.sheet_name,
            ams_topo.ams_sheets.set_id,
            ams_topo.ams_sheets.material_type,
            ams_topo.ams_sheets.index_map,
            ams_topo.ams_sheets.file_format,
            ams_topo.ams_sheets.geom_type,
            ams_topo.ams_set.holding_institution,
            ams_topo.ams_set.rights,
            ams_topo.ams_set.collection,
            ams_topo.ams_set.sierra_id,
            ams_topo.ams_set.set_layer_slug
            FROM ams_topo.ams_sheets
            JOIN ams_topo.ams_footprints ON ams_topo.ams_sheets.gid = ams_topo.ams_footprints.gid
            JOIN ams_topo.ams_set ON ams_topo.ams_sheets.set_id = ams_topo.ams_set.set_id
            WHERE ams_topo.ams_sheets.set_id = %s""",
            (set_id[0],)
        )
        sheet_records = cursor.fetchall()
        for sheet_record in sheet_records:
            # print (sheet_record)
            sheet_id = sheet_record[0]
            identifier_url = sheet_record[1]
            layer_slug = sheet_record[2]
            sheet_name = sheet_record[3]
            set_id = sheet_record[4]
            material_type = sheet_record[5]
            index_map = sheet_record[6]
            file_format = sheet_record[7]
            geom_type = sheet_record[8]
            holding_institution = sheet_record[9]
            rights = sheet_record[10]
            collection = sheet_record[11]
            sierra_id = sheet_record[12]
            set_layer_slug = sheet_record[13]
            # print ('retrieving bounding box')
            if index_map:
                bounding_box_dict = retrieve_bounding_box_for_set(set_id, cursor)
            else:
                cursor.execute(
                    """SELECT ST_AsGeoJSON(ams_topo.ams_footprints.geom)
                    FROM ams_topo.ams_footprints
                    JOIN ams_topo.ams_sheets ON ams_topo.ams_sheets.gid=ams_topo.ams_footprints.gid
                    WHERE ams_topo.ams_sheets.sheet_id = %s
                    """,
                    (sheet_id,)
                )
                try:
                    coor_list = (ast.literal_eval(cursor.fetchone()[0])['coordinates'])
                    for item in coor_list:
                        coordinates = item[0]
                        bounding_box_dict = {
                            'south': coordinates[0][1],
                            'west': coordinates[0][0],
                            'north': coordinates[2][1],
                            'east': coordinates[2][0]
                        }
                except TypeError:
                    continue
            # print ('retrieving place names')
            if index_map:
                place_names = retrieve_place_names_for_set(set_id, cursor)
            else:
                cursor.execute(
                    """SELECT ams_topo.place_names.name
                    FROM ams_topo.place_names
                    JOIN ams_topo.place_lookup ON ams_topo.place_names.place_id = ams_topo.place_lookup.place_id
                    JOIN ams_topo.ams_sheets ON ams_topo.place_lookup.sheet_id=ams_topo.ams_sheets.sheet_id
                    WHERE ams_topo.ams_sheets.sheet_id = %s
                    """,
                    (sheet_id,)
                )
                place_names = []
                places = cursor.fetchall()
                try:
                    for place in places:
                        if place[0] not in place_names:
                            try:
                                name = place[0].encode(encoding='utf-8').decode('ascii')
                                place_names.append(name)
                            except UnicodeDecodeError:
                                if set_layer_slug == 'French-West-Africa-1200000-6559093-10':
                                    print ('***********unicode error', set_layer_slug, sheet_name, sheet_id, place[0], '***********************')
                                pass
                except TypeError:
                    continue
            # print ('retrieving api data')
            # print ('getting token')
            get_token = requests.post(
                'https://catalog2.lib.utexas.edu/iii/sierra-api/v4/token',
                auth=('6duSUiHpeJvGj0v1pLfc9tcQPRSi', 'RDr6^LKj4p')
            ).json()
            api_token = get_token['access_token']
            # print ('token received')
            # print ('calling api with id', sierra_id)
            sierra_record = requests.get(
                'https://catalog2.lib.utexas.edu:443/iii/sierra-api/v4/bibs/{0}?fields=id%2CupdatedDate%2CcreatedDate%2CdeletedDate%2Cdeleted%2Csuppressed%2Cavailable%2Clang%2Ctitle%2Cauthor%2CmaterialType%2CbibLevel%2CpublishYear%2CcatalogDate%2Ccountry%2Corders%2CnormTitle%2CnormAuthor%2Clocations%2CfixedFields%2CvarFields'.format(sierra_id),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer {0}'.format(
                        api_token
                    )
                }
            ).json()
            title = sierra_record['title']
            author = [sierra_record['author']]
            try:
                publish_year = str(sierra_record['publishYear'])
            except KeyError:
                publish_year = ''
            language = sierra_record['lang']['name']
            description = []
            subjects = []
            publisher = []
            # print ('processing varfields')
            for varfield in sierra_record['varFields']:
                try:
                    marc_tag = varfield['marcTag']
                except KeyError:
                    continue
                if marc_tag == '500':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in description:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                description.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
                if marc_tag == '255':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in description:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                description.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
                if marc_tag == '300':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in description:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                description.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
                if marc_tag == '507':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in description:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                description.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
                if marc_tag == '246':
                    for subfield in varfield['subfields']:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            description.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
                if marc_tag == '264':
                    for subfield in varfield['subfields']:
                        try:
                            blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                            publisher.append(blurb.replace('\"', ''))
                        except UnicodeDecodeError:
                            pass
                if marc_tag == '651':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in subjects:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                subjects.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
                if marc_tag == '650':
                    for subfield in varfield['subfields']:
                        if subfield['content'] not in subjects:
                            try:
                                blurb = subfield['content'].encode(encoding='utf-8').decode('ascii')
                                subjects.append(blurb.replace('\"', ''))
                            except UnicodeDecodeError:
                                pass
            sheet_title = '{0} {1}'.format(title, sheet_name)
            print (sheet_title)
            # print ('creating sheet dictionary')
            sheet_dict= {
                "geoblacklight_version": "1.0",
                "dc_identifier_s": identifier_url,
                "layer_slug_s": layer_slug,
                "dc_title_s": sheet_title,
                "solr_geom": "ENVELOPE({0}, {1}, {2}, {3})".format(
                bounding_box_dict['west'],
                bounding_box_dict['east'],
                bounding_box_dict['north'],
                bounding_box_dict['south']
                ),
                "dct_provenance_s": holding_institution,
                "dc_rights_s": rights,
                "layer_geom_type_s": geom_type
            }
            if description:
                sheet_dict["dc_description_s"] = " ".join(description)
            if author:
                sheet_dict["dc_creator_sm"] =  author
            if collection:
                if title:
                    sheet_dict["dct_isPartOf_sm"] = '{0}, {1}'.format(collection, title)
                else:
                    sheet_dict["dct_isPartOf_sm"] = collection
            else:
                if title:
                    sheet_dict["dct_isPartOf_sm"] = title
            if language:
                sheet_dict["dc_language_s"] =  language
            if place_names:
                sheet_dict["dct_spatial_sm"] =  place_names
            if publisher:
                sheet_dict["dc_publisher_s"] =  " ".join(publisher)
            if subjects:
                sheet_dict["dc_subject_sm"] =  subjects
            if material_type:
                sheet_dict["dc_type_s"] =  material_type
            if publish_year:
                sheet_dict["dct_temporal_sm"] = [publish_year]
                sheet_dict["dct_issued_s"] = publish_year
            print ('appending json list')
            json_list.append(sheet_dict)

        outfile_name = 'ams_topo_{0}_sheets.json'.format(set_layer_slug)
        with open(outfile_name, 'w') as outfile:
            json.dump(json_list, outfile)
