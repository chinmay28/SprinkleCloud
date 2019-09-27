import os
import constants as constants
from cloud_io import CloudFactory
from compression import Zipper
from encryption import AesCoder


class RecoveryManager(object):

    @staticmethod
    def xor(file1, file2, destination_file_handle):
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

    @staticmethod
    def get_metadata_file(split_files):
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
