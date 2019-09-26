

class RecoveryManager(object):

    @staticmethod
    def xor(file1, file2, destination_file_handle):
        file1_bytes = bytearray(open(file1, 'rb').read())
        file2_bytes = bytearray(open(file2, 'rb').read())

        if len(file1_bytes) != len(file2_bytes):
            print(len(file1_bytes), len(file2_bytes))
            print('Length of two files not same, we may lose data!')
            return

        print 'XORing {} and {} of length {}...'.format(file1, file2, len(file1_bytes))
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
    def download(meta_pair_entries):
        reconstruct = False
        # try downloading the two files

