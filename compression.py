import os
from zipfile import ZipFile


def zip_file(src_file, dst_file=None, cleanup=True):
    """Compresses a given file."""
    if not dst_file:
        dst_file = "{}.zip".format(src_file)

    with ZipFile(dst_file, "w") as myzip:
        myzip.write(src_file)
        if cleanup:
            os.remove(src_file)
    return dst_file


def unzip_file(src_file, target_dir="./", cleanup=True):
    """Uncompresses a given archive."""
    with ZipFile(src_file, "r") as zip_ref:
        zip_ref.extractall(target_dir)
        if cleanup:
            os.remove(src_file)
    return src_file[:-4]
