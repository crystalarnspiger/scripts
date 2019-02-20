import csv
import psycopg2


def test_fileread():
    with open('place_names.csv') as read_file:
        reader = csv.reader(read_file, delimiter=',')
        for row in reader:
            print (row)


def update_catalog_ids():
    results = []
        connect_string = "dbname='gis' user={0} host={1} password={2}".format(
            local_constants.gis_db_user,
            local_constants.gis_db_host,
            local_constants.gis_db_password
        )
        conn = psycopg2.connect(connect_string)
    print ('successful connection')
    cursor = conn.cursor()
    print ('cursor set')
    with open('data-1510072495841.csv') as read_file:
        reader = csv.DictReader(read_file)
        print ('reading file')
        for row in reader:
            print ('reading rows')
            file_id = row['file_id']
            catalog_id = row['catalog_id']
            print (file_id, catalog_id)
            cursor.execute(
                """UPDATE ams_topo.ams_file
                SET catalog_id = (%s)
                WHERE (file_id = %s)
                RETURNING file_id""",
                (catalog_id, file_id,)
            )
            print ('updated record', cursor.fetchone()[0])
    print ('committing changes')
    conn.commit()
    print ('done')
