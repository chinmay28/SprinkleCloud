import os
import pickle
import pprint

import constants as constants
from cloud_io import CloudFactory
from compression import Zipper
from encryption import AesCoder


class RecoveryManager(object):

    SPECIAL_TOKENS = (constants.XOR, constants.DUP)

    def __init__(self, file_name, cloud_writers, encryption_key):
        """ We populate remote file pieces and local file metadata,
        and recover any  pieces that may be missing """
        self.encryption_key = encryption_key
        self.source_file_name = file_name
        self.remote_file_pieces = []
        self.metadata = {}
        self.reconstruction_map = {}

        self._discover_remote_file_pieces(file_name, cloud_writers)
        if not self.remote_file_pieces:
            raise Exception('File information for {} not found '
                            'in in remote clouds!'.format(file_name))

        self._load_metadata(file_name)
        if not self.metadata:
            raise Exception('File information for {} not found '
                            'in metadata file!'.format(file_name))

        self.reconstruct_files()

    def _discover_remote_file_pieces(self, file_name, cloud_writers):
        print('Discover file parts...')
        self.remote_file_pieces = [file_object for cloud in cloud_writers
                                   for file_object in cloud.list()
                                   if file_name in file_object['name']]

        print('Discovered {} file pieces'.format(len(self.remote_file_pieces)))
        pprint.pprint(self.remote_file_pieces)

    def _load_metadata(self, file_name):
        print('Loading file metadata...')
        with open('metadata.pickle', 'rb') as mfile:
            all_metadata = pickle.load(mfile)

        self.metadata = all_metadata['files'].get(file_name)
        print 'File Metadata for {}:'.format(file_name)
        pprint.pprint(self.metadata)

    def reconstruct_files(self):
        """ Iterates through local metadata and remote pieces and reconstructs
        any pieces that might be missing or lost """
        local_file_names = [file_entry['name'] for entries in self.metadata.values()
                            for file_entry in entries]
        if len(local_file_names) == len(self.remote_file_pieces):
            print('All file pieces intact in the remote clouds. '
                  'No reconstruction necessary.')
            return

        remote_file_names = [piece['name'] for piece in self.remote_file_pieces]
        self.reconstruction_map = {file_name: file_name in remote_file_names
                                   for file_name in local_file_names}
        print('File map for {}:'.format(self.source_file_name))
        pprint.pprint(self.reconstruction_map)

        for file_name, file_exists in self.reconstruction_map.iteritems():
            if not file_exists:
                self.remote_reconstruct(file_name)

    def remote_reconstruct(self, file_name):
        """ Reconstructs specific file and puts it back on the remote cloud """
        print('Reconstructing {}...'.format(file_name))
        metadata_list = next(entry_list for entry_list in self.metadata.values()
                             if any(file_name == file_entry['name']
                                    for file_entry in entry_list))

        # download the supporting files
        supporting_files = []
        for file_entry in metadata_list:
            if file_name == file_entry['name']:
                continue  # this file is the one we need to reconstruct
            # TODO: fix inefficient linear id fetching
            file_id = next(remote_file['id'] for remote_file in self.remote_file_pieces
                           if remote_file['name'] == file_entry['name'])

            # if it is a dup file all we need to do is copy and upload
            if any(constants.DUP in name for name in [file_entry['name'], file_name]):
                out_file = file_name
            else:
                out_file = file_entry['name']
            supporting_files.append(file_entry['name'])

            with open(out_file, 'wb') as sfile:
                CloudFactory.get_cloud(file_entry['cloud']).download(file_id, sfile)

        if not any(constants.DUP in name for name in [supporting_files[0], file_name]):
            # unzip
            unzipped_files = []
            for sup_file in supporting_files:
                unzipped_files.append(Zipper.unzip(sup_file))

            # decrypt
            dec_filenames = []
            for unzip_file in unzipped_files:
                dec_filenames.append(unzip_file[:-4])
                AesCoder.decrypt_file(self.encryption_key, unzip_file, unzip_file[:-4])

            bad_file_name = file_name[:-8]
            with open(bad_file_name, 'wb') as bad_file:
                self.xor(dec_filenames[0], dec_filenames[1], bad_file, cleanup=True)

            # encrypt
            AesCoder.encrypt_file(self.encryption_key, bad_file_name,
                                  '{}.enc'.format(bad_file_name))
            # compress
            Zipper.zip('{}.enc'.format(bad_file_name),
                       '{}.enc.zip'.format(bad_file_name))
            assert file_name == '{}.enc.zip'.format(bad_file_name)

        # upload
        bad_file = next(file_meta for file_meta in metadata_list
                        if file_meta['name'] == file_name)
        cloud_conn = CloudFactory.get_cloud(bad_file['cloud'])
        # TODO upload to a random cloud and update the metadata
        cloud_conn.upload(file_name, file_name, cleanup=True)

    @staticmethod
    def xor(file1, file2, destination_file_handle, cleanup=False):
        file1_bytes = bytearray(open(file1, 'rb').read())
        file2_bytes = bytearray(open(file2, 'rb').read())

        if len(file1_bytes) != len(file2_bytes):
            print(len(file1_bytes), len(file2_bytes))
            raise Exception('Length of two files not same, we may lose data!')

        print 'XORing {} and {} of length {} into {}...'.format(
            file1, file2, len(file1_bytes), destination_file_handle.name)
        result = bytearray()
        for i in xrange(len(file1_bytes)):
            result.append(file1_bytes[i] ^ file2_bytes[i])

        destination_file_handle.write(result)
        if cleanup:
            os.remove(file1)
            os.remove(file2)

    @staticmethod
    def setup_file_recovery(split_files):
        """ When the file pieces are ready, we setup recovery and generate metadata """
        metadata_dict = {}

        file_count = len(split_files)
        if file_count % 2:
            # duplicate/mirror the file
            orig_file = split_files[-1]
            dup_file = '{}.dup'.format(split_files[-1])
            with open(orig_file, 'rb') as orig, open(dup_file, 'wb') as dup:
                dup.write(orig.read())

            metadata_dict[split_files[-1]] = [
                {'name': orig_file},
                {'name': dup_file},
            ]
            file_count -= 1

        for i in xrange(0, file_count, 2):
            xor_file_name = '{}.xor'.format(split_files[i])
            with open(xor_file_name, 'wb') as xor_file:
                RecoveryManager.xor(split_files[i], split_files[i+1], xor_file)

            metadata_dict[split_files[i]] = [
                {'name': split_files[i]},
                {'name': split_files[i+1]},
                {'name': xor_file_name}
            ]

        return metadata_dict

    @staticmethod
    def download(discovered_files, meta_pair_values, encryption_key):
        reconstruct = {}
        xor_file = next((file_meta for file_meta in meta_pair_values
                        if constants.XOR in file_meta['name']), None)
        dup_file = next((file_meta for file_meta in meta_pair_values
                        if constants.DUP in file_meta['name']), None)

        special_tokens = (constants.XOR, constants.DUP)

        data_files = [entry for entry in meta_pair_values
                      if all(token not in entry['name'] for token in special_tokens)]

        # try downloading the two files
        for entry in meta_pair_values:
            if all(token not in entry['name'] for token in special_tokens):

                matching_file = next((file_object for file_object in discovered_files
                                     if file_object['name'] == entry['name']), None)
                if not matching_file:
                    reconstruct[entry['name']] = True
                else:
                    cloud_conn = CloudFactory.get_cloud(entry['cloud'])
                    with open(entry['name'], 'wb') as dfile:
                        try:
                            cloud_conn.download(file_id=matching_file['id'],
                                                local_file_handle=dfile)
                        except Exception as exc:
                            print exc
                            reconstruct[entry['name']] = True
                            continue

                    if os.path.getsize(entry['name']) != entry['size']:
                        reconstruct[entry['name']] = True

        if reconstruct:
            if len(reconstruct) > 1:
                raise Exception('More than one file has gone bad. Cannot recover!')

            bad_file_name = reconstruct.keys()[0]
            print('Reconstructing {}...'.format(bad_file_name))

            if dup_file:
                matching_file = next(file_object for file_object in discovered_files
                                     if file_object['name'] == dup_file['name'])
                cloud_conn = CloudFactory.get_cloud(dup_file['cloud'])

                with open(bad_file_name, 'wb') as bad_file:
                    cloud_conn.download(file_id=matching_file['id'], local_file_handle=bad_file)

            elif xor_file:
                good_file = next(file_meta for file_meta in meta_pair_values
                                 if file_meta['name'] != bad_file_name)

                matching_file = next(file_object for file_object in discovered_files
                                     if file_object['name'] == xor_file['name'])

                cloud_conn = CloudFactory.get_cloud(xor_file['cloud'])
                with open(xor_file['name'], 'wb') as xor:
                    cloud_conn.download(file_id=matching_file['id'], local_file_handle=xor)

                if os.path.getsize(xor_file['name']) != xor_file['size']:
                    raise Exception('XOR file may be corrupted. Expected size {}, actual size: {}'
                                    .format(os.path.getsize(xor_file['name']), xor_file['size']))

                # unzip
                xor_file_name = Zipper.unzip(xor_file['name'])
                good_file_name = Zipper.unzip(good_file['name'], cleanup=False)

                # decrypt
                xor_file_name = AesCoder.decrypt_file(
                    encryption_key, xor_file_name, xor_file_name[:-4])
                good_file_name = AesCoder.decrypt_file(
                    encryption_key, good_file_name, good_file_name[:-4])

                bad_file_name = bad_file_name[:-8]
                with open(bad_file_name, 'wb') as bad_file:
                    RecoveryManager.xor(good_file_name, xor_file_name, bad_file)
                os.remove(xor_file_name)

                # encrypt
                AesCoder.encrypt_file(encryption_key, bad_file_name,
                                      '{}.enc'.format(bad_file_name))
                # compress
                Zipper.zip('{}.enc'.format(bad_file_name), '{}.enc.zip'.format(bad_file_name))

                # upload
                bad_file = next(file_meta for file_meta in meta_pair_values
                                if file_meta['name'] == '{}.enc.zip'.format(bad_file_name))
                cloud_conn = CloudFactory.get_cloud(bad_file['cloud'])
                # TODO upload to a random cloud and update the metadata
                cloud_conn.upload(bad_file['name'], bad_file['name'])
            else:
                print('Reconstruction failed. Redundant file not found!')

        return data_files
