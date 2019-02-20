import copy
import csv

from utilities import data_list

import local_constants


def parse_gis():
    pclmaps_digitized = []
    pclmaps_digitized_parsed = {}
    pclmaps_gis = []
    pclmaps_old_digitized = []
    pclmaps_parsed_full = [[
        'column_1',
        'georeferenced',
        'clipped',
        'oclc',
        'file_name_digitization',
        'file',
        'filepath',
        'path_digitization',
        'url_files',
        'url_html',
        'size_kb',
        'size_mb',
        'collection',
        'subcollection',
        'batch_internal_id_digitization',
        'number_images_in_batch_digitization',
        'number_items_in_batch_digitization',
        'archived_digitization',
        'assigned_digitization',
        'billing_account_digitization',
        'contact_info_digitization',
        'content_type_digitization',
        'created_digitization',
        'created_by_digitization',
        'date_received_digitization',
        'file_names_from_batch_digitization',
        'format_digitization',
        'format_other_digitization',
        'id_digitization',
        'image_gallery_digitization',
        'job_phase_digitization',
        'description_digitization',
        'item_type_digitization',
        'project_id_old',
        'client_old',
        'contact_old',
        'funding_source_old',
        'no_file_old',
        'capture_type_old',
        'format_description_old',
        'capture_start_date_old',
        'date_completed_old',
        'description2_old',
        'possible_file_name_digitized_old',
        'capture_time_old',
        'int_or_ext_old',
        'item_type_old',
        'path_old',
    ]]
    pclmaps_parsed_ready_for_work = [[
        'Column 1',
        'Georeferenced',
        'Clipped',
        'Worth Finding',
        'Worth Providing as Service',
        'OCLC',
        'File Name',
        'File With Format',
        'Possible File Name - Old',
        'Filepath',
        'Digitization Filepath',
        'File Path - Old',
        'URL Files',
        'URL HTML',
        'Size KB',
        'Size MB',
        'Collection',
        'Subcollection',
        'Digitization Batch ID',
        'Project ID - Old',
        'Archived by Digitization',
        'Original Format',
        'Original Format Other',
        'Original Format - Old',
        'Description',
        'Description Old',
        'Image Gallery URL',
    ]]
    with open('completed.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            if row['internal_id'].startswith('pclmaps'):
                pclmaps_digitized.append(row)
    for item in pclmaps_digitized:
        filenames = item['file_names'].replace('\n', ' ')
        filenames = filenames.replace('\r', ' ')
        filenames = filenames.split(' ')
        for filename in filenames:
            if filename == '':
                continue
            elif filename == 'SIP':
                continue
            elif filename == 'Root':
                continue
            elif filename in ['2017_0104','2017_0105','2017_0106','2017_0107','2017_0108','2017_0109']:
                continue
            oclc = ''
            filename_split = filename.split('-')
            try:
                oclc_index = filename_split.index('oclc')
                oclc_index += 1
                oclc = filename_split[oclc_index]
            except ValueError:
                pass
            item['file_name'] = filename
            item['oclc'] = oclc
            pclmaps_digitized_parsed[filename] = item
    with open('pcl_maps_list_17jun17.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            pclmaps_gis.append(row)
    full_dict = {}
    oclc_dict = {}
    for item in pclmaps_gis:
        filename_split = item['filepath'].split('/')
        filename_split = filename_split[-1].split('.')
        filename = filename_split[0]
        try:
            digitized = pclmaps_digitized_parsed[filename]
        except KeyError:
            digitized = {
                'number_images': '',
                'number_items': '',
                'archived': '',
                'assigned': '',
                'billing_account': '',
                'contact_info': '',
                'type': '',
                'created': '',
                'created_by': '',
                'date_received': '',
                'file_name': filename,
                'oclc': '',
                'file_names': '',
                'format': '',
                'format_other': '',
                'id': '',
                'image_gallery': '',
                'internal_id': '',
                'job_phase': '',
                'description': '',
                'item_type': '',
                'path': ''
            }
            file_split2 = filename.split('-')
            try:
                oclc_index = file_split2.index('oclc')
                oclc_index += 1
                oclc = file_split2[oclc_index]
                digitized['oclc'] = oclc
            except ValueError:
                pass
        new_item = {}
        new_item.update(item)
        new_item.update(digitized)
        full_dict[filename] = new_item
        if new_item['oclc']:
            oclc_dict[new_item['oclc']] = new_item
    for filename in pclmaps_digitized_parsed:
        try:
            found = full_dict[filename]
        except KeyError:
            new_item = {}
            new_item.update(pclmaps_digitized_parsed[filename])
            new_item.update(
                {
                    'column_1': '',
                    'size_kb': '',
                    'size_mb': '',
                    'filepath': '',
                    'url_files': '',
                    'url_html': '',
                    'collection': '',
                    'subcollection': '',
                    'file': '',
                    'georeferenced': '',
                    'clipped': ''
                }
            )
            full_dict[filename] = new_item
            if new_item['oclc']:
                oclc_dict[new_item['oclc']] = new_item
    with open('old.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            if row['project_id'].startswith('pclmaps'):
                pclmaps_old_digitized.append(row)
    not_found_old = []
    for item in pclmaps_old_digitized:
        found_record = ''
        oclc = ''
        item['file_name_digitized_old'] = ''
        description = item['description2'].split(' ')
        for entry in description:
            try:
                found_record = full_dict[entry]
                found_record['file_name_digitized_old'] = entry
                new_item = {}
                new_item.update(item)
                new_item.update(found_record)
                full_dict[entry] = new_item
            except KeyError:
                try:
                    found_record = oclc_dict[entry]
                    found_record['file_name_digitized_old'] = ''
                    new_item = {}
                    new_item.update(item)
                    new_item.update(found_record)
                    full_dict[found_record['file_name']] = new_item
                except KeyError:
                    pass
            if entry == 'OCLC':
                oclc_index = description.index(entry)
                oclc_index += 1
                oclc = description[oclc_index]
            if '-' in entry and 'batch' not in entry:
                item['file_name_digitized_old'] = entry
        if not found_record:
            try:
                not_found_index = not_found_old.index(item)
            except ValueError:
                new_item = {}
                new_item.update(item)
                new_item.update({
                    'number_images': '',
                    'number_items': '',
                    'archived': '',
                    'assigned': '',
                    'billing_account': '',
                    'contact_info': '',
                    'type': '',
                    'created': '',
                    'created_by': '',
                    'date_received': '',
                    'file_name': '',
                    'oclc': oclc,
                    'file_names': '',
                    'format': '',
                    'format_other': '',
                    'id': '',
                    'image_gallery': '',
                    'internal_id': '',
                    'job_phase': '',
                    'description': '',
                    'item_type': '',
                    'path': '',
                    'column_1': '',
                    'size_kb': '',
                    'size_mb': '',
                    'filepath': '',
                    'url_files': '',
                    'url_html': '',
                    'collection': '',
                    'subcollection': '',
                    'file': '',
                    'georeferenced': '',
                    'clipped': '',

                })
                not_found_old.append(new_item)
    for filename in full_dict:
        try:
            record_full = full_dict[filename]['project_id']
        except KeyError:
            new_item = {}
            new_item.update({
                'project_id': '',
                'client': '',
                'contact': '',
                'funding_source': '',
                'no_file': '',
                'capture_type': '',
                'format_description': '',
                'capture_start_date': '',
                'date_completed': '',
                'description2': '',
                'file_name_digitized_old': '',
                'capture_time': '',
                'int_or_ext': '',
                'item_type_old': '',
                'path_old': '',
            })
            new_item.update(full_dict[filename])
            full_dict[filename] = new_item

        pclmaps_parsed_full.append([
            full_dict[filename]['column_1'],
            full_dict[filename]['georeferenced'],
            full_dict[filename]['clipped'],
            full_dict[filename]['oclc'],
            full_dict[filename]['file_name'],
            full_dict[filename]['file'],
            full_dict[filename]['filepath'],
            full_dict[filename]['path'],
            full_dict[filename]['url_files'],
            full_dict[filename]['url_html'],
            full_dict[filename]['size_kb'],
            full_dict[filename]['size_mb'],
            full_dict[filename]['collection'],
            full_dict[filename]['subcollection'],
            full_dict[filename]['internal_id'],
            full_dict[filename]['number_images'],
            full_dict[filename]['number_items'],
            full_dict[filename]['archived'],
            full_dict[filename]['assigned'],
            full_dict[filename]['billing_account'],
            full_dict[filename]['contact_info'],
            full_dict[filename]['type'],
            full_dict[filename]['created'],
            full_dict[filename]['created_by'],
            full_dict[filename]['date_received'],
            full_dict[filename]['file_names'],
            full_dict[filename]['format'],
            full_dict[filename]['format_other'],
            full_dict[filename]['id'],
            full_dict[filename]['image_gallery'],
            full_dict[filename]['job_phase'],
            full_dict[filename]['description'],
            full_dict[filename]['item_type'],
            full_dict[filename]['project_id'],
            full_dict[filename]['client'],
            full_dict[filename]['contact'],
            full_dict[filename]['funding_source'],
            full_dict[filename]['no_file'],
            full_dict[filename]['capture_type'],
            full_dict[filename]['format_description'],
            full_dict[filename]['capture_start_date'],
            full_dict[filename]['date_completed'],
            full_dict[filename]['description2'],
            full_dict[filename]['file_name_digitized_old'],
            full_dict[filename]['capture_time'],
            full_dict[filename]['int_or_ext'],
            full_dict[filename]['item_type_old'],
            full_dict[filename]['path_old'],
        ])

        pclmaps_parsed_ready_for_work.append([
            full_dict[filename]['column_1'],
            full_dict[filename]['georeferenced'],
            full_dict[filename]['clipped'],
            '',
            '',
            full_dict[filename]['oclc'],
            full_dict[filename]['file_name'],
            full_dict[filename]['file'],
            full_dict[filename]['file_name_digitized_old'],
            full_dict[filename]['filepath'],
            full_dict[filename]['path'],
            full_dict[filename]['path_old'],
            full_dict[filename]['url_files'],
            full_dict[filename]['url_html'],
            full_dict[filename]['size_kb'],
            full_dict[filename]['size_mb'],
            full_dict[filename]['collection'],
            full_dict[filename]['subcollection'],
            full_dict[filename]['internal_id'],
            full_dict[filename]['project_id'],
            full_dict[filename]['archived'],
            full_dict[filename]['format'],
            full_dict[filename]['format_other'],
            full_dict[filename]['format_description'],
            full_dict[filename]['description'],
            full_dict[filename]['description2'],
            full_dict[filename]['image_gallery'],
        ])

    for item in not_found_old:
        pclmaps_parsed_full.append([
            item['column_1'],
            item['georeferenced'],
            item['clipped'],
            item['oclc'],
            item['file_name'],
            item['file'],
            item['filepath'],
            item['path'],
            item['url_files'],
            item['url_html'],
            item['size_kb'],
            item['size_mb'],
            item['collection'],
            item['subcollection'],
            item['internal_id'],
            item['number_images'],
            item['number_items'],
            item['archived'],
            item['assigned'],
            item['billing_account'],
            item['contact_info'],
            item['type'],
            item['created'],
            item['created_by'],
            item['date_received'],
            item['file_names'],
            item['format'],
            item['format_other'],
            item['id'],
            item['image_gallery'],
            item['job_phase'],
            item['description'],
            item['item_type'],
            item['project_id'],
            item['client'],
            item['contact'],
            item['funding_source'],
            item['no_file'],
            item['capture_type'],
            item['format_description'],
            item['capture_start_date'],
            item['date_completed'],
            item['description2'],
            item['file_name_digitized_old'],
            item['capture_time'],
            item['int_or_ext'],
            item['item_type_old'],
            item['path_old'],
        ])

        pclmaps_parsed_ready_for_work.append([
            item['column_1'],
            item['georeferenced'],
            item['clipped'],
            '',
            '',
            item['oclc'],
            item['file_name'],
            item['file'],
            item['file_name_digitized_old'],
            item['filepath'],
            item['path'],
            item['path_old'],
            item['url_files'],
            item['url_html'],
            item['size_kb'],
            item['size_mb'],
            item['collection'],
            item['subcollection'],
            item['internal_id'],
            item['project_id'],
            item['archived'],
            item['format'],
            item['format_other'],
            item['format_description'],
            item['description'],
            item['description2'],
            item['image_gallery'],
        ])


    with open('pclmaps_full.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for row in pclmaps_parsed_full:
            writer.writerow(row)

    with open('pclmaps_working.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for row in pclmaps_parsed_ready_for_work:
            writer.writerow(row)
