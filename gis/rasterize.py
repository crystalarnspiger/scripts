import csv
import os
import subprocess

import local_constants

db_name = local_constants.gis_db_name
db_host = local_constants.gis_db_host
db_user = local_constants.gis_db_user
db_password = local_constants.gis_db_password


def rastermethis():
    os.environ['PGPASSWORD'] = db_password

    with open('../../../../Volumes/gis/pclmaps_working_georefed.csv') as read_file:
        reader = csv.DictReader(read_file)
        for row in reader:
            print (row['GEOFilePath'])
            if row['rastered'] == 'rastered':
                print ('rastered')
                continue
            geofilepath = '../../../..{0}'.format(row['GEOFilePath'])
            cmd = 'raster2pgsql -s 4326 -a -F {0} ams_topo.ams_raster | psql -U {1} -d {2} -h {3} -p 5432'.format(geofilepath,db_user,db_name,db_host)
            subprocess.call(cmd, shell=True)


def rg_points():
    os.environ['PGPASSWORD'] = db_password

    geofilepath = '../../../..{0}'.format('RG_Points.shp')
    cmd = 'raster2pgsql -s 4326 -a -F {0} ams_topo.ams_raster | psql -U {1} -d {2} -h {3} -p 5432'.format(geofilepath,db_user,db_name,db_host)
    subprocess.call(cmd, shell=True)
