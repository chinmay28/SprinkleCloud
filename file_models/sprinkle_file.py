import random

import constants
from cloud_io import CloudFactory
from compression import zip_file, unzip_file
from file_models.file_piece import FilePiece
import recovery


class SprinkleFile(object):
    """Models the target file to be uploaded/downloaded"""
    def __init__(self, filename, encryption_key=constants.DEFAULT_ENCRYPTION_KEY,
                 metadata=None, recovery_algorithm=None):
        self.filename = filename
        self.zipped_filename = "{}.zip".format(filename)
        self.encryption_key = "{:$^32}".format(encryption_key)
        self.metadata = {"pieces": {}, "merge_order": []} if not metadata else metadata

        if recovery_algorithm:
            self.recovery_algorithm = recovery_algorithm
        else:
            self.recovery_algorithm = recovery.xor_raid4_file_recovery

    def upload(self):
        """Sprinkles a given file onto the clouds."""
        zip_file(self.filename, self.zipped_filename)
        merge_order = recovery.split_file(filename=self.zipped_filename)
        self.metadata["merge_order"] = merge_order

        all_clouds = CloudFactory.get_cloud_names()
        file_pieces = [
            FilePiece(
                piece_name,
                self.filename,
                random.choice(all_clouds)
            ) for piece_name in merge_order
        ]

        # setup recovery
        file_pieces = self.recovery_algorithm(file_pieces)

        for piece in file_pieces:
            piece.encrypt(self.encryption_key)
            piece.zip()
            piece.upload()
            self.metadata["pieces"][piece.name] = piece.metadata

    def download(self):
        """Gets the sprinkled file from the clouds."""
        file_pieces = []  # for keeping pieces for the lifetime of this method
        for name, file_meta in self.metadata["pieces"].iteritems():
            piece = FilePiece(name, self.filename, metadata=file_meta)
            piece.download()
            piece.unzip()
            piece.decrypt(self.encryption_key)
            file_pieces.append(piece)

        recovery.merge_files(self.metadata["merge_order"],
                             destination_filename=self.zipped_filename)
        unzip_file(src_file=self.zipped_filename)
