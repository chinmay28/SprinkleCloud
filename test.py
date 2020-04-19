import os
import pickle
import pprint
import time

from cloud_io import CloudFactory
from file_models.sprinkle_file import SprinkleFile


if __name__ == '__main__':

    source_files = [
        "tc_extra_small.png",
        "tc_small.pdf",
        "tc_medium.pdf",
        "tc_large.mp4",
        "tc_corner.jpg",
    ]
    encryption_key = "{}{}".format("B@n@n@C@k3", time.time())
    cloud_writers = CloudFactory.get_cloud_connections()

    print('Deleting all files...')
    for conn in cloud_writers:
        files = [file_object for file_object in conn.list()]
        for file_object in files:
            conn.delete(file_object['id'])
        conn.list()
    if os.path.exists('metadata.pickle'):
        os.remove('metadata.pickle')

    # upload
    for source_file in source_files:
        sfile = SprinkleFile(source_file, encryption_key=encryption_key)
        sfile.upload()
        metadata = {}
        if os.path.exists('metadata.pickle'):
            with open('metadata.pickle', 'rb') as metafile:
                metadata = pickle.load(metafile)
        metadata[source_file] = sfile.metadata

        with open('metadata.pickle', 'wb') as metafile:
            pickle.dump(metadata, metafile)

    print("*"*20)
    print("File metadata")
    print("*" * 20)
    pprint.pprint(metadata)
    print("*" * 20)

    # download
    metadata = {}
    for source_file in source_files:
        with open('metadata.pickle', 'rb') as metafile:
            metadata = pickle.load(metafile)

        dfile = SprinkleFile(source_file, metadata=metadata[source_file],
                             encryption_key=encryption_key)
        dfile.download()
