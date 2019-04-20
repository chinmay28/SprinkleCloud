import os
import pickle
from logging import debug, info

import googleapiclient
from google.auth.transport import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import http, errors
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from constants import *


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

    def put(self, name, path, mime_type='application/pdf', metadata=None):
        pass

    def get(self, content):
        pass

    def download(self, file_id, local_file_handle):
        pass

    def delete(self, file_id):
        pass

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

    def put(self, name, path, mime_type='application/pdf', parent_id=None, metadata=None):
        if metadata:
            metadata.update({'name': name})
        else:
            metadata = {'name': name}

        if parent_id:
            metadata['parents'] = [parent_id]
        else:
            metadata['parents'] = [self.parent_folder_id]

        super(GDriveWriter, self).put(name, path)

        media = MediaFileUpload(path, mimetype=mime_type)
        file = self.service.files().create(body=metadata, media_body=media, fields='id').execute()
        debug('File ID: {}'.format(file.get('id')))
        return file

    def delete(self, file_id):
        super(GDriveWriter, self).delete(file_id)
        print('Deleting file with ID {}'.format(file_id))
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
        return items


if __name__ == '__main__':
    gdrive = GDriveWriter()
    gdrive.list()
    # gdrive.download(file_id='1Bjh2oomDGJ5dbQMsnPp7aWqdIpV_4_Sp', local_file_handle=open('test.zip', 'wb'))
    # gdrive.put('TunnelBear', 'TunnelBear.zip', mime_type='application/zip')
    # for file_id in ['1qNxaQbJR1OpY9MfqAWvsr8WRv5gBH9Ws', '1X48gq7GKMeSuy9ini_rtaCXJLpVux9x7', '1GLIRPyjT8WQblnVZ3Uqi5rH1OSUhntMu',
    #                 '1Bjh2oomDGJ5dbQMsnPp7aWqdIpV_4_Sp']:
    # gdrive.delete('1dJJt6Jq85lsBmNexUtxBGXKUj7ji9JJq')
    gdrive.list(folder_id=gdrive.parent_folder_id)
