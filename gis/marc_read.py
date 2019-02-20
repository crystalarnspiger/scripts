import csv

from pymarc import MARCReader, marcxml


def print_marc_record(record):
    print (record)

def read_marc():
    digitized_oclc = []
    records = []
    with open('pclmaps_working_georefed.csv', 'r') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            if row['OCLC']:
                digitized_oclc.append(row['OCLC'])

    with open('mapsgis.mrc', 'rb') as read_file:
        reader= MARCReader(read_file)
        oclc_list = []
        for record in reader:
            oclc = ''
            try:
                if record['001']:
                    oclc = record['001']['a']
                    oclc = oclc.replace('ocm', '')
                    oclc = oclc.replace('ocn', '')
                    oclc_list.append(oclc)
                if oclc in digitized_oclc:
                    records.append(record)
            except TypeError:
                pass

    set_digitized_oclc = set(digitized_oclc)
    set_marc_records = set(oclc_list)
    in_both = set_digitized_oclc.intersection(set_marc_records)
    print ('digitized_oclc: {0}'.format(set_digitized_oclc))
    print ('marc_records: {0}'.format(set_marc_records))
    print ('in both: {0}'.format(in_both))


    with open('mapsgis.txt', 'w') as write_file:
        for record in records:
            write_file.write(str(record))
