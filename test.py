import os
import sys

from encryption import AesCoder
from cloud_io import GDriveWriter
from parts import PartManager, Zipper

if __name__ == '__main__':
    operation = sys.argv[1]
    key = '{:$^32}'.format(sys.argv[2])
    source_file = sys.argv[3]

    if operation == 'upload':
        print('Compress file {}...'.format(source_file))
        Zipper.zip(src_file=source_file, dst_file='{}.zip'.format(source_file))
        source_file = '{}.zip'.format(source_file)

        print('Split file {}...'.format(source_file))
        count = PartManager.split(path=source_file, prefix=source_file)

        print('Encrypt {} file parts...'.format(count - 1))
        for i in range(1, count):
            file_name = '{}_{}'.format(source_file, i)
            AesCoder.encrypt_file(key, in_filename=file_name,
                                  out_filename='{}_{}.enc'.format(source_file, i))

        print('Compress {} file parts...'.format(count - 1))
        for i in range(1, count):
            file_name = '{}_{}.enc'.format(source_file, i)
            Zipper.zip(src_file=file_name, dst_file='{}.zip'.format(file_name))

        print('Upload {} file parts...'.format(count - 1))
        gdrive = GDriveWriter()
        for i in range(1, count):
            file_name = '{}_{}.enc.zip'.format(source_file, i)
            gdrive.put(file_name, file_name, mime_type='application/zip')
            os.remove(file_name)
        gdrive.list()

    elif operation == 'clean':
        print('Deleting all files...')
        gdrive = GDriveWriter()
        files = [file_object for file_object in gdrive.list()]
        for file_object in files:
            gdrive.delete(file_object['id'])
        gdrive.list()

    else:
        source_file = '{}.zip'.format(source_file)
        print('Discover file parts...')
        gdrive = GDriveWriter()
        files = [file_object for file_object in gdrive.list() if source_file in file_object['name']]

        import pprint
        pprint.pprint(files)
        print(len(files))

        print('Downloading files...')
        for file_obj in files:
            with open(file_obj['name'], 'wb') as part_file:
                gdrive.download(file_id=file_obj['id'], local_file_handle=part_file)

        print('Decompress {} part files...'.format(len(files)))
        for i, file_obj in enumerate(files):
            Zipper.unzip(src_file=file_obj['name'])

        print('Decrypt {} part files...'.format(len(files)))
        for file_obj in files:
            AesCoder.decrypt_file(key, file_obj['name'][:-4], out_filename=file_obj['name'][:-8])

        print('Merge {} part files...'.format(len(files)))
        PartManager.merge(source_file, source_file, len(files))

        print('Decompress merged file...')
        Zipper.unzip(src_file=source_file)
