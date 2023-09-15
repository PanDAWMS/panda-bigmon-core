"""
The wrapper handle connection and to load log files from Object Store

@author: Tatiana Korchuganova
"""
import boto3
import logging
import os

from django.conf import settings

_logger = logging.getLogger('bigpandamon-filebrowser')

class ObjectStore(object):
    host_name = None
    port = None
    bucket_name = None
    _access_key = None
    _secret_key = None
    client = None
    resource = None

    def __init__(self):

        if hasattr(settings, 'OBJECT_STORE'):
            self.host_name = settings.OBJECT_STORE['host_name'] if 'host_name' in settings.OBJECT_STORE else None
            self.port = settings.OBJECT_STORE['port'] if 'port' in settings.OBJECT_STORE else None
            self.bucket_name = settings.OBJECT_STORE['bucket_name'] if 'bucket_name' in settings.OBJECT_STORE else None
            self._access_key = settings.OBJECT_STORE['access_key'] if 'access_key' in settings.OBJECT_STORE else None
            self._secret_key = settings.OBJECT_STORE['secret_key'] if 'secret_key' in settings.OBJECT_STORE else None

    def init_client(self):
        if self.host_name and self.port and self.bucket_name and self._access_key and self._secret_key:
            self.client = boto3.client(
                service_name='s3',
                endpoint_url='https://{}:{}'.format(self.host_name, self.port),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key
            )
        else:
            self.client = None
            _logger.error('No credentials for connecting to object store found in settings')

    def init_resource(self):
        if self.host_name and self.port and self.bucket_name and self._access_key and self._secret_key:
            self.resource = boto3.resource(
                service_name='s3',
                endpoint_url='https://{}:{}'.format(self.host_name, self.port),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key
            )
        else:
            self.resource = None
            _logger.error('No credentials for connecting to object store found in settings')

    def is_bucket(self):
        pass

    def get_fullpath(self):

        return 'https://{}:{}/{}/PandaJob_{}/'

    def get_folder_path(self, pandaid=None, computingsite=None):
        if not pandaid:
            _logger.warning('No pandaid provided to ')
        if not computingsite:
            _logger.warning('No computingsite provided')
        if pandaid and computingsite:
            return '{}/PandaJob_{}/'.format(computingsite, pandaid)
        else:
            return None

    def download_folder(self, s3_folder, local_dir=None):
        """
        Download the contents of a folder directory
        Args:
            s3_folder: the folder path in the s3 bucket
            local_dir: the absolute directory path in the local file system
        """
        error_str = ''
        if not self.resource:
            self.init_resource()
        if self.resource:
            bucket = self.resource.Bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=s3_folder):
                filename = str(obj.key).split('/')[-1]
                target = '{}{}'.format(local_dir, filename)
                if os.path.isfile(target):
                    _logger.debug('Log file already exists, skipping download {}'.format(target))
                    continue
                try:
                    bucket.download_file(obj.key, target)
                except Exception as ex:
                    _logger.exception('Failed to download {} to {} with:\n{}'.format(filename, target, str(ex)))
                    error_str += 'Failed to download file from ObjectStore'
        else:
            _logger.warning('Failed to init resource, can not download folder with logs')
            error_str += 'Failed to obtain connection with ObjectStore'
        return error_str

    def download_file(self, object_name=None, local_dir=None):
        if object_name and local_dir:
            rt = self.client.download_file(
                self.bucket_name,
                object_name,
                '{}/{}'.format(local_dir, object_name)
            )
        pass
