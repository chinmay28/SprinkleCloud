import os
import pickle
from logging import debug, info

import dropbox
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import http, errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from boxsdk import DevelopmentClient

from compression import unzip_file, zip_file
from constants import *
from encryption import AesCoder


class CloudWriter(object):

    BASE_FOLDER = 'private_files'

    def __init__(self):
        self.name = BASE_CLOUD
        self.creds = None
        self.service = None
        self.tokens = {}

    def setup_access(self):
        info('Setting up access for {}...'.format(self.name))
        file_name = self.tokens['access']
        if os.path.exists(file_name):
            debug('Access token file {} exists. Loading it...')
            with open(file_name, 'rb') as token:
                self.creds = pickle.load(token)
        else:
            debug('Access token file {} does not exist.')

    def connect(self):
        pass

    def upload(self, name, path, mime_type='application/zip', metadata=None, cleanup=False):
        # TODO: implement this cleanly as pre and post upload actions
        if cleanup:
            os.remove(path)

    def get(self, content):
        pass

    def download(self, file_id, local_file_handle):
        print 'Downloading file with id {}...'.format(file_id)

    def delete(self, file_id):
        print('Deleting file with path {}'.format(file_id))

    def list(self):
        pass


class GDriveWriter(CloudWriter):

    SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self):
        super(GDriveWriter, self).__init__()
        self.name = GOOGLE_DRIVE
        self.tokens = {
            'access': 'tokens/{}.pickle'.format(self.name),
            'credentials': 'tokens/{}_credentials.json'.format(self.name)
        }
        self.parent_folder_id = None

        self.setup_access()
        self.connect()

    def setup_access(self):
        super(GDriveWriter, self).setup_access()
        if not self.creds:
            credentials_file = self.tokens['credentials']
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file,
                                                             GDriveWriter.SCOPES)
            self.creds = flow.run_local_server()
            # Save the credentials for the next run
            with open(self.tokens['access'], 'wb') as token:
                pickle.dump(self.creds, token)

    def connect(self):
        super(GDriveWriter, self).connect()
        self.service = build('drive', 'v3', credentials=self.creds, cache_discovery=False)

        # load private_files folder
        parent_folder = {
            'name': CloudWriter.BASE_FOLDER,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        self.parent_folder_id = next((file_obj['id'] for file_obj in self.list(silent=True)
                                      if parent_folder['name'] == file_obj['name']), None)
        if not self.parent_folder_id:
            self.parent_folder_id = self.service.files().create(
                body=parent_folder, fields='id').execute()

    def upload(self, name, path, mime_type='application/zip', parent_id=None,
               metadata=None, cleanup=False):
        print 'Uploading {}...'.format(path)
        if metadata:
            metadata.update({'name': name})
        else:
            metadata = {'name': name}

        if parent_id:
            metadata['parents'] = [parent_id]
        else:
            metadata['parents'] = [self.parent_folder_id]

        media = MediaFileUpload(path, mimetype=mime_type)
        file = self.service.files().create(body=metadata, media_body=media, fields='id').execute()
        debug('File ID: {}'.format(file.get('id')))
        super(GDriveWriter, self).upload(name, path, cleanup=cleanup)
        return file["id"]

    def delete(self, file_id):
        super(GDriveWriter, self).delete(file_id)
        self.service.files().delete(fileId=file_id).execute()

    def get(self, file_id):
        """ Print a file's metadata. """
        return self.service.files().get(fileId=file_id).execute()

    def download(self, file_id, local_file_handle):
        """ Download a Drive file's content to the local filesystem """
        request = self.service.files().get_media(fileId=file_id)
        media_request = http.MediaIoBaseDownload(local_file_handle, request)

        while True:
            try:
                download_progress, done = media_request.next_chunk()
            except errors.HttpError as exc:
                print('An error occurred: %s' % exc)
                return
            if download_progress:
                print('Download Progress: %d%%' % int(download_progress.progress() * 100))
            if done:
                print('Download Complete')
                return

    def list(self, folder_id=None, silent=False):
        super(GDriveWriter, self).list()
        kwargs = dict(fields="nextPageToken, files(id, name)")
        if folder_id:
            kwargs['q'] = "'{}' in parents".format(folder_id)

        results = self.service.files().list(**kwargs).execute()
        items = results.get('files', [])

        if not silent:
            if not items:
                print('No files found.')
                return None
            else:
                print('Files:')
                for item in items:
                    print(u'{0} ({1})'.format(item['name'], item['id']))
        return items if items else []


class DropboxWriter(CloudWriter):

    def __init__(self):
        super(DropboxWriter, self).__init__()
        self.name = DROPBOX
        self.tokens = {
            'access': 'tokens/{}.pickle'.format(self.name),
            'credentials': 'tokens/{}_access_token.txt'.format(self.name)
        }

        self.setup_access()
        self.connect()

    def setup_access(self):
        super(DropboxWriter, self).setup_access()
        if not self.creds:
            with open(self.tokens['credentials'], 'r') as credentials_file:
                self.creds = {
                    'access-token': credentials_file.readline().strip()
                }
            with open(self.tokens['access'], 'wb') as token:
                pickle.dump(self.creds, token)

    def connect(self):
        super(DropboxWriter, self).connect()
        self.service = dropbox.Dropbox(self.creds['access-token'])

    def upload(self, name, path, mime_type='application/zip', parent_id=None,
               metadata=None, cleanup=False):
        print 'Uploading {}...'.format(path)
        if metadata:
            metadata.update({'name': name})
        else:
            metadata = {'name': name}

        with open(path, 'rb') as file_handle:
            file_meta = self.service.files_upload(file_handle.read(),
                                                  '/{}/{}'.format(self.BASE_FOLDER, name))
        super(DropboxWriter, self).upload(name, path, cleanup=cleanup)
        return file_meta.id

    def delete(self, file_id):
        super(DropboxWriter, self).delete(file_id)
        self.service.files_delete_v2(file_id)

    def get(self, file_id):
        """ Print a file's metadata. """
        # TODO find a better, faster way to do this
        for entry in self.list():
            if entry['id'] == file_id:
                return entry
        raise Exception('File with id {} Not found!'.format(file_id))

    def download(self, file_id, local_file_handle):
        """ Download a Drive file's content to the local filesystem """
        super(DropboxWriter, self).download(file_id, local_file_handle)
        _, response = self.service.files_download(file_id)
        local_file_handle.write(response.content)

    def list(self, folder_id=None, silent=False):
        super(DropboxWriter, self).list()
        if folder_id is None:
            folder_id = '/{}'.format(self.BASE_FOLDER)
        files = [{'name': item.name, 'id': item.id,
                  'path': '/{}/{}'.format(self.BASE_FOLDER, item.name)}
                 for item in self.service.files_list_folder(folder_id).entries]
        return files


class BoxWriter(CloudWriter):

    def __init__(self):
        self.name = BOX
        self.service = DevelopmentClient()
        self.parent_folder = next(
            folder_info for folder_info in self.service.root_folder().get_items()
            if folder_info.name == self.BASE_FOLDER)

    def setup_access(self):
        pass

    def connect(self):
        pass

    def upload(self, name, path, mime_type='application/zip', parent_id=None,
               metadata=None, cleanup=False):
        print 'Uploading {}...'.format(path)
        file_object = self.parent_folder.upload(file_name=name, file_path=path)
        super(BoxWriter, self).upload(name, path, cleanup=cleanup)
        return file_object.id

    def delete(self, file_id):
        super(BoxWriter, self).delete(file_id)
        self.service.file(file_id).delete()

    def get(self, file_id):
        """ Print a file's metadata. """
        file_object = self.service.file(file_id).get()
        return {'id': file_object.id, 'name': file_object.name}

    def download(self, file_id, local_file_handle):
        """ Download a Drive file's content to the local filesystem """
        super(BoxWriter, self).download(file_id, local_file_handle)
        self.service.file(file_id).download_to(local_file_handle)

    def list(self, folder_id=None, silent=False):
        super(BoxWriter, self).list()
        files = [{'name': item.name, 'id': item.id}
                 for item in self.parent_folder.get().item_collection['entries']]
        return files


class CloudFactory(object):

    CLOUDS = {
        BOX: BoxWriter(),
        DROPBOX: DropboxWriter(),
        GOOGLE_DRIVE: GDriveWriter()
    }

    @classmethod
    def get_cloud(cls, name):
        if name in cls.CLOUDS:
            return cls.CLOUDS[name]
        raise Exception('Cloud {} not supported'.format(name))

    @classmethod
    def get_cloud_connections(cls):
        return [cls.CLOUDS[name] for name in cls.CLOUDS]

    @classmethod
    def get_cloud_names(cls):
        return cls.CLOUDS.keys()


class Metadata(object):
    # TODO: this should subclass dict

    FILENAME = "metadata.pickle"
    CLOUD_ALIAS = "{}.enc.zip".format(FILENAME)
    ENCRYPTION_KEY = "{:$^32}".format(METADATA_ENCRYTPION_KEY)

    @classmethod
    def load(cls):
        """Loads the metadata from the cloud."""
        for cloud_conn in CloudFactory.get_cloud_connections():

            try:
                metadata_file = next((file_object for file_object in cloud_conn.list()
                                      if file_object['name'] == cls.CLOUD_ALIAS), None)
            except TypeError:
                # TODO: this is stupid
                continue

            if metadata_file:
                with open(metadata_file['name'], 'wb') as zipped_file:
                    cloud_conn.download(metadata_file['id'], zipped_file)

                encrypted_filename = unzip_file(metadata_file['name'])
                AesCoder.decrypt_file(key=cls.ENCRYPTION_KEY,
                                      in_filename=encrypted_filename, out_filename=cls.FILENAME)
                break
        else:
            print('Metadata file could not be found on any clouds!')
            return {}

        with open(cls.FILENAME, 'rb') as meta_file:
            meta = pickle.load(meta_file)

        return meta

    @classmethod
    def store(cls, metadata_dict):
        """Stores the metadata onto the clouds."""
        with open(cls.FILENAME, 'wb') as metadata_file:
            pickle.dump(metadata_dict, metadata_file)

        encrypted_filename = AesCoder.encrypt_file(cls.ENCRYPTION_KEY, in_filename=cls.FILENAME)
        zip_file(encrypted_filename, dst_file=cls.CLOUD_ALIAS)

        for cloud_conn in CloudFactory.get_cloud_connections():
            try:
                metadata_file = next((file_object for file_object in cloud_conn.list()
                                      if file_object['name'] == cls.CLOUD_ALIAS), None)
            except TypeError:
                # TODO: this is stupid
                metadata_file = None

            if metadata_file:
                cloud_conn.delete(metadata_file['id'])
            cloud_conn.upload(cls.CLOUD_ALIAS, cls.CLOUD_ALIAS)


if __name__ == '__main__':
    drive = DropboxWriter()
    import pprint
    pprint.pprint(drive.get('JyjNdS08-9AAAAAAAAADZQ'))
    # pprint.pprint(Metadata.load())
    # gdrive.download(file_id='1Bjh2oomDGJ5dbQMsnPp7aWqdIpV_4_Sp', local_file_handle=open('test.zip', 'wb'))
    # gdrive.upload('TunnelBear', 'TunnelBear.zip', mime_type='application/zip')
    # for file_id in ['1qNxaQbJR1OpY9MfqAWvsr8WRv5gBH9Ws', '1X48gq7GKMeSuy9ini_rtaCXJLpVux9x7', '1GLIRPyjT8WQblnVZ3Uqi5rH1OSUhntMu',
    #                 '1Bjh2oomDGJ5dbQMsnPp7aWqdIpV_4_Sp']:
    # gdrive.delete('1dJJt6Jq85lsBmNexUtxBGXKUj7ji9JJq')
    # gdrive.list(folder_id=gdrive.parent_folder_id)
