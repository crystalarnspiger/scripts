import csv
import os
from pathlib import Path


def list_files():
    spreadsheet_files = []
    with open('../../../../Volumes/gis/pclmaps_working_georefed.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            spreadsheet_files.append(row['File_Name'])

    rootdir = Path('../../../../Volumes/gis/AMS_Topo')
    
    file_list = [f for f in rootdir.resolve().glob('**/*') if f.is_file()]

    all_files = []
    found_files = {}
    exception_files =[]

    for file_path in file_list:
        if file_path.name.endswith('c.tif'):
            all_files.append(file_path)
            file_split = os.path.split(file_path)
            file_name = str(file_split[-1])
            file_name_start = file_name.split('_')[0]
            if file_name_start in spreadsheet_files:
                found_files[file_name_start] = [file_name_start,file_name, file_path, '']
                print ('yes, file: {0}'.format(file_name))
            else:
                exception_files.append(file_path)
                print ('no, file: {0}'.format(file_name))
        elif file_path.name.endswith('.tif'):
            all_files.append(file_path)
            file_split = os.path.split(file_path)
            file_name = str(file_split[-1])
            file_name_start = file_name.split('_')[0]
            if file_name_start in spreadsheet_files:
                try:
                    confirm = found_files[file_name_start]
                except KeyError:
                    found_files[file_name_start] = [file_name_start, file_name, file_path, 'not clipped']
                    print ('yes, file: {0}'.format(file_name))
            else:
                exception_files.append(file_path)
                print ('no, file: {0}'.format(file_name))

    with open('allfiles.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for file_path in all_files:
            writer.writerow([file_path])

    with open('pclnames.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for file_name in spreadsheet_files:
            try:
                print (found_files[file_name])
                row = found_files[file_name]
                print (row)
            except KeyError:
                row = [file_name, '', '', '']
                print ('exception: {0}'.format(file_name))
            writer.writerow(row)
    with open('pclmisnames.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for file_name in exception_files:
            row = [file_name]
            writer.writerow(row)
