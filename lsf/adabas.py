import codecs
import csv


def correct_files():
    print ('here')
    print ('files gotten')
    print ('correcting item file')
    with open('item_data.txt','r') as read_file:
        reader = csv.reader((line.replace('\0','') for line in read_file), delimiter='|')
        with open('corrected_items.csv', 'w') as write_file:
            writer = csv.writer(write_file, delimiter='|')
            x = 0
            for record in reader:
                print (record)
                right_way = record[-1][:5]
                print (right_way)
                record[-1] = right_way
                print (record[-1])
                print ('writing row', x)
                writer.writerow(record)
                if len(record) > 31:
                    print (len(record))
                x += 1
    print ('correcting container file')
    with open('container_data.txt', 'r') as read_file:
        reader = csv.reader((line.replace('\0','') for line in read_file), delimiter='|')
        with open('corrected_containers.csv', 'w') as write_file:
            writer = csv.writer(write_file, delimiter='|')
            x = 0
            for record in reader:
                print (record)
                right_way = record[-1][:5]
                record[-1] = right_way
                print ('writing row', x)
                writer.writerow(record)
                if len(record) > 31:
                    print (len(record))
                x += 1
    print ('success i think')

def read_containers():
    containers = {}
    print ('reading container file')
    with open('corrected_containers.csv', 'r') as read_file:
        reader = csv.DictReader(read_file, delimiter='|')
        print (reader)
        for record in reader:
            print (record)
            container_number = record['CONTAINNMBR']
            if container_number in ['0244650','244650']:
                print (record)
            # print ('parsing container', container_number)
            containers[container_number] = {
                'type': record['T'],
                'container_number': record['CONTAINNMBR'],
                'owning_org': record['OWNO'],
                'row': record['ROWNB'],
                'ladder': record['LADDN'],
                'shelf': record['SHELF'],
                'size_type': record['Z'],
                'weight': record['CTWEIGHT'],
                'format': record['F'],
                'processing_status': record['P'],
                'date_added': record['DADDED'],
                'date_billed': record['DBILLD'],
                'date_deleted': record['DDELET'],
                'delete': record['W'],
                'shelf_height': record['HEIGT'],
                'shelf_type': record['Y'],
                'catalog_location': record['CALOC']
            }

    # return containers


def read_items():
    items = []
    print ('reading item file')
    with open('corrected_items.csv', 'r') as read_file:
        reader = csv.DictReader(read_file, delimiter='|')
        for record in reader:
            # print ('parsing item', record['INV NBR'])
            if record['CONTAINER'] in ['0244650','244650']:
                print (record)
            items.append(
                {
                    'inventory_number': record['INV NBR'],
                    'owning_unit_item_number': str(record['OWNING ID']),
                    'inventory_container_number': record['CONTAINER'],
                    'box_or_tray': record['B'],
                    'description': record['DESCRIPTION'],
                    'desc_keyword1': record['KEYWORD1'],
                    'desc_keyword2': record['KEYWORD2'],
                    'desc_keyword3': record['KEYWORD3'],
                    'desc_keyword4': record['KEYWORD4'],
                    'desc_keyword5': record['KEYWORD5'],
                    'box_vol_ser_nbr': record['BVS-NBR'],
                    'owning_unit_code': record['OWNU'],
                    'non_gl_sw': record['G'],
                    'restricted_use_sw': record['R'],
                    'date_added_to_storage': record['ADDED'],
                    'last_activity_date': record['LACTIV'],
                    'discard_date': record['DSCRD'],
                    'last_activity_code': record['C'],
                    'requesting_unit_code': record['REQU'],
                    'requestor_name': record['REQUESTOR NAME'],
                    'requestor_id': record['REQ ID'],
                    'reshelving_sw': record['S'],
                    'total_retrievals': record['TOTRET'],
                    'total_emergency_retrievals': record['TOTER'],
                    'delete_sw': record['D'],
                    'emergency_retrieval_sw': record['E'],
                    'sierra_loc': record['SILOC'],
                    'call_number': record['CALL NUMBER'],
                    'pickup_loc': record['PCKUP'],
                    'digitize': record['Z'],
                    'resource_in_common': record['M'],
                }
            )

    # return items


def readadabas():
    print ('correcting files')
    correct_files
    print ('reading containers')
    containers = read_containers()
    print ('reading items')
    items = read_items()
    print ('combining records')
    exceptions = []
    for item in items:
        try:
            container = containers[item['inventory_container_number']]
        except KeyError:
            exceptions.append((item['inventory_number'], item['inventory_container_number']))
            continue

        print ('combining item {0} to container {1}'.format(
            item['inventory_number'],
            container['container_number']
        ))

        item['cnt_type'] = container['type']
        item['cnt_container_number'] = container['container_number']
        item['cnt_owning_org'] = container['owning_org']
        item['cnt_row'] = container['row']
        item['cnt_ladder'] = container['ladder']
        item['cnt_shelf'] = container['shelf']
        item['cnt_size_type'] = container['size_type']
        item['cnt_weight'] = container['weight']
        item['cnt_format'] = container['format']
        item['cnt_processing_status'] = container['processing_status']
        item['cnt_date_added'] = container['date_added']
        item['cnt_date_billed'] = container['date_billed']
        item['cnt_date_deleted'] = container['date_deleted']
        item['cnt_delete'] = container['delete']
        item['cnt_shelf_height'] = container['shelf_height']
        item['cnt_shelf_type'] = container['shelf_type']
        item['cnt_catalog_location'] = container['catalog_location']

    with open ('full_records.csv', 'w') as write_file:
        print ('file open')
        print ('writing headers')
        fieldnames = items[0].keys()
        writer = csv.DictWriter(write_file, fieldnames, delimiter=',')
        writer.writeheader()
        print ('writing rows')
        for item in items:
            print ('writing row', item['inventory_number'])
            writer.writerow(item)

    print (exceptions)
    print ('done')
