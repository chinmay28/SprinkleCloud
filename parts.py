import os
from zipfile import ZipFile


class PartManager(object):

    CHUNK_SIZE = 1024 * 1024

    @staticmethod
    def split(path, prefix=None):
        if not prefix:
            prefix = path
        file_number = 1
        with open(path, 'rb') as src_file:
            chunk = src_file.read(PartManager.CHUNK_SIZE)
            while chunk:
                with open('{}_{}'.format(prefix, file_number), 'wb') as chunk_file:
                    chunk_file.write(chunk)
                file_number += 1
                chunk = src_file.read(PartManager.CHUNK_SIZE)
        return file_number

    @staticmethod
    def merge(path, prefix, count, cleanup=True):
        with open(path, 'wb') as dest_file:
            for i in range(1, count + 1):
                with open('{}_{}'.format(prefix, i), 'rb') as chunk_file:
                    chunk = chunk_file.read()
                    dest_file.write(chunk)
                    if cleanup:
                        os.remove('{}_{}'.format(prefix, i))


class Zipper(object):

    @staticmethod
    def zip(src_file, dst_file, cleanup=True):
        with ZipFile(dst_file, 'w') as myzip:
            myzip.write(src_file)
            if cleanup:
                os.remove(src_file)

    @staticmethod
    def unzip(src_file, target_dir="./", cleanup=True):
        with ZipFile(src_file, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
            if cleanup:
                os.remove(src_file)


if __name__ == '__main__':
    Zipper.zip('tmp/src_file.pdf', 'test.zip')
    import time
    time.sleep(60)
    Zipper.unzip(src_file='test.zip', target_dir='tmp')
