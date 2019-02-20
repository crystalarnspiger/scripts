import paramiko

import local_constants


def test_sftp():
    print ('starting transport')
    lsf = paramiko.Transport((local_constants.lsf_server))
    print ('connecting')
    lsf.connect(
        username=local_constants.lsf_username,
        password=local_constants.lsf_password
    )
    print ('starting sftp')
    sftp = paramiko.SFTPClient.from_transport(lsf)
    print ('getting file')
    sftp.get('store.180808', 'store_test')
    print ('done, closing')

    lsf.close()


def readfile():
    print ('opening file')
    with open('store_test') as readfile:
        entries = readfile.read()
        print ('reading file')
        # for entry in entries:
        #     print (entry)
        print (entries)
    print ('done')
