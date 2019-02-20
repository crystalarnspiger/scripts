import csv
import os

from pathlib import Path


def get_size():
    spreadsheet_files = []
    with open('../../../../Volumes/gis/pclmaps_working_georefed.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            spreadsheet_files.append(row['File_Name'])
    file_sizes = [
        [
            'file_path',
            'file_name',
            'file_size',
            'file_xml',
            'file_aux_xml',
            'file_ovr',
            'file_size_in_MB',
            'total_size_in_MB'
        ]
    ]
    rootdir = Path('../../../../Volumes/gis/AMS_Topo')
    file_list = [f for f in rootdir.resolve().glob('**/*') if f.is_file()]
    for file_path in file_list:
        if file_path.name.endswith('c.tif'):
            file_path_split = os.path.split(file_path)
            if 'compressed' in file_path_split[0:]:
                continue
            file_name = file_path_split[-1]
            file_name_start = file_name.split('_')[0]
            try:
                file_xml_size = os.path.getsize('{0}{1}'.format(file_path,'.xml'))
            except FileNotFoundError:
                file_xml_size = 0
            try:
                file_auxxml_size = os.path.getsize('{0}{1}'.format(file_path,'.aux.xml'))
            except FileNotFoundError:
                file_auxxml_size = 0
            try:
                file_ovr_size = os.path.getsize('{0}{1}'.format(file_path,'.ovr'))
            except FileNotFoundError:
                file_ovr_size = 0
            if file_name_start in spreadsheet_files:
                print (
                    'file name: {0}\n'\
                    'file size: {1}\n'\
                    'file xml: {2}\n'\
                    'file aux xml: {3}\n'\
                    'file ovr: {4}\n'\
                    'file size in MB: {5}\n'\
                    'total size in MB: {6}'.format(
                        file_name,
                        os.path.getsize(file_path),
                        file_xml_size,
                        file_auxxml_size,
                        file_ovr_size,
                        os.path.getsize(file_path)//(1024*1024),
                        (
                            os.path.getsize(file_path) +
                            file_xml_size +
                            file_auxxml_size +
                            file_ovr_size
                        )/(1024*1024)
                    )
                )
                file_sizes.append(
                    [
                        file_path,
                        file_name,
                        os.path.getsize(file_path),
                        file_xml_size,
                        file_auxxml_size,
                        file_ovr_size,
                        os.path.getsize(file_path)//(1024*1024),
                        (
                            os.path.getsize(file_path) +
                            file_xml_size +
                            file_auxxml_size +
                            file_ovr_size
                        )//(1024*1024)
                    ]
                )
    with open('file_sizes.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',')
        for row in file_sizes:
            writer.writerow(row)
