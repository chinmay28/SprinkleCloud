import os
import pickle
import random
import sys
import pprint

from encryption import AesCoder
from cloud_io import CloudFactory
from compression import PartManager, Zipper
from recovery import RecoveryManager

if __name__ == '__main__':
    operation = sys.argv[1]
    encryption_key = '{:$^32}'.format(sys.argv[2])
    source_file = sys.argv[3]

    cloud_writers = CloudFactory.get_all_clouds()

    if operation == 'upload':
        print('Compress file {}...'.format(source_file))
        Zipper.zip(src_file=source_file, dst_file='{}.zip'.format(source_file), cleanup=False)
        source_file_zipped = '{}.zip'.format(source_file)

        print('Split file {}...'.format(source_file_zipped))
        count = PartManager.split(path=source_file_zipped, prefix=source_file_zipped)
        os.remove(source_file_zipped)

        print('Get metadata and prepare redundancy...')
        split_files = ['{}_{}'.format(source_file_zipped, i) for i in xrange(1, count + 1)]
        metadata = RecoveryManager.get_metadata_file(split_files)

        print 'Metadata:'
        pprint.pprint(metadata)

        print('Encrypt {} file parts...'.format(count))
        for _, files in metadata.iteritems():
            for data_file in files:
                AesCoder.encrypt_file(encryption_key, in_filename=data_file['name'],
                                      out_filename='{}.enc'.format(data_file['name']))
                data_file['name'] = '{}.enc'.format(data_file['name'])

        print('Compress {} file parts...'.format(count))
        for _, files in metadata.iteritems():
            for data_file in files:
                Zipper.zip(src_file=data_file['name'], dst_file='{}.zip'.format(data_file['name']))
                data_file['name'] = '{}.zip'.format(data_file['name'])
                data_file['size'] = os.path.getsize(data_file['name'])

        print('Upload files...')
        for _, files in metadata.iteritems():
            connections = cloud_writers
            random.shuffle(connections)
            for i, data_file in enumerate(files):
                writer = connections[i]
                data_file['cloud'] = writer.name
                writer.upload(data_file['name'], data_file['name'], mime_type='application/zip')

        print('Cleaning up temp local files...')
        for _, files in metadata.iteritems():
            for data_file in files:
                os.remove(data_file['name'])

        if os.path.exists('metadata.pickle'):
            with open('metadata.pickle', 'rb') as mfile:
                all_metadata = pickle.load(mfile)
        else:
            all_metadata = {'files': {}}

        all_metadata['files'][source_file] = metadata

        print 'Metadata file:'
        pprint.pprint(all_metadata)
        with open('metadata.pickle', 'wb') as mfile:
            pickle.dump(all_metadata, mfile)

    elif operation == 'clean':
        print('Deleting all files...')
        for conn in cloud_writers:
            files = [file_object for file_object in conn.list()]
            for file_object in files:
                conn.delete(file_object['id'])
            conn.list()
        os.remove('metadata.pickle')

    else:
        print('Discover file parts...')
        with open('metadata.pickle', 'rb') as mfile:
            all_metadata = pickle.load(mfile)

        metadata = all_metadata['files'].get(source_file)
        print 'Metadata:'
        pprint.pprint(metadata)

        if not metadata:
            print('File information for {} not found in metadata file!'.format(source_file))
            exit(0)

        print('Downloading files...')
        for key, meta_pair_entries in metadata.iteritems():
            RecoveryManager.download(meta_pair_entries)



        # print('Downloading files...')
        # for file_obj in files:
        #     with open(file_obj['name'], 'wb') as part_file:
        #         gdrive.download(file_id=file_obj['id'], local_file_handle=part_file)
        #
        # print('Decompress {} part files...'.format(len(files)))
        # for i, file_obj in enumerate(files):
        #     Zipper.unzip(src_file=file_obj['name'])
        #
        # print('Decrypt {} part files...'.format(len(files)))
        # for file_obj in files:
        #     AesCoder.decrypt_file(encryption_key, file_obj['name'][:-4], out_filename=file_obj['name'][:-8])
        #
        # print('Merge {} part files...'.format(len(files)))
        # PartManager.merge(source_file, source_file, len(files))
        #
        # print('Decompress merged file...')
        # Zipper.unzip(src_file=source_file)
