import os

from cloud_io import CloudFactory
from constants import XOR, DUP
from file_models.file_piece import FilePiece

# Size of split files in bytes
CHUNK_SIZE = 1024 * 1024


def split_file(filename, prefix=None, cleanup=True):
    """Splits a given file into a number of pieces."""
    if not prefix:
        prefix = filename
    file_number = 1
    with open(filename, 'rb') as src_file:
        chunk = src_file.read(CHUNK_SIZE)
        while chunk:
            with open('{}_{}'.format(prefix, file_number), 'wb') as chunk_file:
                chunk_file.write(chunk)
            file_number += 1
            chunk = src_file.read(CHUNK_SIZE)

    if cleanup:
        os.remove(filename)
    return ['{}_{}'.format(prefix, i) for i in xrange(1, file_number)]


def merge_files(piece_names, destination_filename, cleanup=True):
    """Merges the given file pieces into one."""
    with open(destination_filename, 'wb') as dest_file:
        for piece in piece_names:
            with open(piece, 'rb') as chunk_file:
                chunk = chunk_file.read()
                dest_file.write(chunk)
                if cleanup:
                    os.remove(piece)


def xor_files(file1, file2, destination_file_handle, cleanup=False):
    """XORs the given two files onto a destination file handle."""
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


def xor_raid4_file_recovery(file_pieces):
    """XOR based RAID4 file recovery algorithm."""
    all_clouds = CloudFactory.get_cloud_names()
    xor_pieces = []
    dup_pieces = []

    def create_dup_file(file_piece):
        """Creates duplicate file for recovery."""
        orig_file_name = file_piece.name
        dup_file_name = '{}.dup'.format(file_piece.name)
        with open(orig_file_name, 'rb') as orig, open(dup_file_name, 'wb') as dup:
            dup.write(orig.read())

        target_cloud = next(cloud for cloud in all_clouds if cloud != file_piece.cloud)
        dup_piece = FilePiece(dup_file_name, file_pieces[-1].parent_file,
                              target_cloud, piece_type=DUP)

        file_piece.siblings = [dup_piece.name]
        dup_piece.siblings = [file_piece.name]
        return dup_piece

    file_count = len(file_pieces)
    if file_count and file_count % 2 == 0:
        # It is very likely the last two pieces are not of the same size.
        # Let's simply mirror the files.
        piece1 = create_dup_file(file_pieces[-1])
        piece2 = create_dup_file(file_pieces[-2])
        dup_pieces.extend([piece1, piece2])
        file_count -= 2  # already processed

    elif file_count % 2:
        # we have one last piece that has an unequal size
        dup_piece = create_dup_file(file_pieces[-1])
        dup_pieces.append(dup_piece)
        file_count -= 1  # already processed

    for i in xrange(0, file_count, 2):
        xor_file_name = '{}.xor'.format(file_pieces[i].name)
        with open(xor_file_name, 'wb') as xor_file:
            xor_files(file_pieces[i].name, file_pieces[i + 1].name, xor_file)

        clouds_taken = (file_pieces[i].cloud, file_pieces[i+1].cloud)
        target_cloud = next(cloud for cloud in all_clouds
                            if all(cloud != taken for taken in clouds_taken))
        xor_piece = FilePiece(xor_file_name, file_pieces[i].parent_file,
                              target_cloud, piece_type=XOR)

        xor_piece.siblings = [file_pieces[i].name, file_pieces[i + 1].name]
        file_pieces[i].siblings = [file_pieces[i + 1].name, xor_piece.name]
        file_pieces[i + 1].siblings = [xor_piece.name, file_pieces[i].name]
        xor_pieces.append(xor_piece)

    file_pieces.extend(dup_pieces)
    file_pieces.extend(xor_pieces)

    return file_pieces


def reconstruct(file_piece, metadata, encryption_key):
    """Reconstruct a lost piece with sibling pieces"""
    print("Reconstructing {} from its sibling(s) {}..."
          .format(file_piece.name, file_piece.siblings))
    siblings = []
    for sibling_name in file_piece.siblings:
        sibling_meta = metadata['pieces'][sibling_name]
        sibling = FilePiece(sibling_name, file_piece.parent_file, metadata=sibling_meta)
        if not sibling.exists_in_cloud:
            raise Exception('Data loss detected! Sibling could not be found on cloud.')
        sibling.download()
        sibling.unzip()
        sibling.decrypt(encryption_key)
        siblings.append(sibling)

    if len(siblings) == 2:
        with open(file_piece.name, 'wb') as xor_file:
            xor_files(siblings[0].name, siblings[1].name, xor_file)

    elif len(siblings) == 1:
        with open(file_piece.name, 'wb') as missing_file:
            with open(siblings[0].name, 'rb') as dup_file:
                missing_file.write(dup_file.read())
    else:
        raise Exception("Invalid sibling configuration found! Siblings: {}."
                        .format(file_piece.siblings))

    file_piece.encrypt(encryption_key=encryption_key)
    file_piece.zip()
    file_piece.upload()
    return file_piece.metadata
