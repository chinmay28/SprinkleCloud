import os

from cloud_io import CloudFactory
from encryption import AesCoder
import constants
from compression import zip_file, unzip_file


class FilePiece(object):
    """Represents a piece of a file"""

    def __init__(self, piece_name, parent_file, cloud=None,
                 piece_type=constants.REGULAR, metadata=None):
        """Constructor"""
        self.name = piece_name
        self.encrypted_name = "{}.enc".format(self.name)
        self.zipped_name = '{}.zip'.format(self.encrypted_name)

        self.parent_file = parent_file
        self.cloud = cloud
        self.piece_type = piece_type
        self.metadata = metadata

        self._files_to_cleanup = set()
        self.cloud_alias = "blah"
        self.encryption = "AES"  # for applying a behavioral pattern

        self.siblings = []

        if metadata:
            self.cloud = metadata["cloud"]
            self.piece_type = metadata["piece_type"]
            self.cloud_alias = metadata["cloud_alias"]
            self.siblings = metadata["siblings"]

        self.available_locally = False

    @property
    def cloud_connection(self):
        return CloudFactory.get_cloud(self.cloud)

    @property
    def exists_in_cloud(self):
        """Checks if the file exists in cloud"""
        if not self.metadata:
            return False
        try:
            _ = self.cloud_connection.get(self.metadata["file_id"])['id']
            return True
        except Exception as exc:
            tokens = ('item is trashed', 'not found')
            if any(token in str(exc).lower() for token in tokens):
                return False
            raise

    def encrypt(self, encryption_key):
        """Encrypts the file piece"""
        print "Encrypting {} to {}...".format(self.name, self.encrypted_name)
        AesCoder.encrypt_file(encryption_key, in_filename=self.name,
                              out_filename=self.encrypted_name)
        self.available_locally = False

    def decrypt(self, encryption_key):
        """Decrypts the file piece"""
        print "Decrypting {} to {}...".format(self.encrypted_name, self.name)
        AesCoder.decrypt_file(encryption_key, self.encrypted_name,
                              out_filename=self.name)
        self.available_locally = True

    def zip(self):
        """compresses the encrypted file piece"""
        zip_file(src_file=self.encrypted_name, dst_file=self.zipped_name, cleanup=True)

    def unzip(self):
        """uncompresses the downloaded piece into encrypted file piece"""
        unzip_file(self.zipped_name)

    def upload(self):
        """Uploads the file piece"""
        file_id = self.cloud_connection.upload(self.zipped_name, self.zipped_name)
        self.metadata = {
            "cloud": self.cloud,
            "piece_type": self.piece_type,
            "file_id": file_id,
            "cloud_alias": self.cloud_alias,
            "siblings": self.siblings
        }
        self._files_to_cleanup.add(self.zipped_name)

    def download(self):
        """Downloads the file piece"""
        if not self.metadata or not self.metadata["file_id"]:
            raise Exception("Required metadata not found!")

        with open(self.zipped_name, "wb") as zipped_file:
            self.cloud_connection.download(file_id=self.metadata["file_id"],
                                           local_file_handle=zipped_file)

    def __del__(self):
        """Cleans up local files"""
        for file_piece in self._files_to_cleanup:
            if os.path.exists(file_piece):
                print("Removing {}...".format(file_piece))
                os.remove(file_piece)
