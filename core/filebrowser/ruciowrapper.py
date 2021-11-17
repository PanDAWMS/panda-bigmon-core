from rucio.client import Client, downloadclient
import logging
from core.common.models import RucioAccounts
from django.utils import timezone
from datetime import datetime, timedelta
import os
from .utils import get_rucio_account, get_x509_proxy
from core.filebrowser.utils import get_fullpath_filebrowser_directory, get_filebrowser_directory
import uuid
_logger = logging.getLogger('bigpandamon-filebrowser')


class ruciowrapper(object):
    if 'RUCIO_ACCOUNT' not in os.environ:
        os.environ['RUCIO_ACCOUNT'] = get_rucio_account()
    if 'X509_USER_PROXY' not in os.environ:
        os.environ['X509_USER_PROXY'] = get_x509_proxy()

    client = None
    def __init__(self):
        try:
            self.client = Client()
        except Exception as e:
            logging.error('Failed to initiate Rucio client:' + str(e))

    def download_ds(self, ds_name):
        try:
            dclient = downloadclient.DownloadClient(self.client)
            basedir = get_fullpath_filebrowser_directory() +'/'+ str(uuid.uuid3(uuid.NAMESPACE_URL, ds_name))+'/'
            dclient.download_dids([{'did':ds_name,
                                    'base_dir':basedir}])
        except Exception as e:
            logging.error('Failed to download: ' + ds_name +' ' + str(e))
            return {'exception': 'Failed to download: ' + ds_name +' ' + str(e)}
        return {'exception':None, 'basedir':basedir+'/'+ds_name}

    def getRucioAccountByDN(self, DN):
        values = ['rucio_account', 'create_time']
        accounts = []
        accounts.extend(RucioAccounts.objects.filter(certificatedn=DN).values(*values))
        accountExists = len(accounts)
        if accountExists == 0 or (timezone.now() - accounts[0]['create_time']) > timedelta(days=7):
            if not self.client is None:
                try:
                    accounts = [account['account'] for account in self.client.list_accounts(account_type='USER',identity=DN)]
                except Exception as e:
                    logging.error('Failed to get accounts' + str(e))
                    return accounts

                if len(accounts) > 0:
                    if (accountExists == 0):
                        RucioAccounts.objects.filter(certificatedn=DN).delete()

                    for account in accounts:
                        accountRow = RucioAccounts(
                            rucio_account = account,
                            certificatedn = DN,
                            create_time = timezone.now().date(),
                        )
                        accountRow.save()
        else:
            accounts = [account['rucio_account'] for account in accounts]
        return accounts

    def getRSEbyDID(self, dids):
        """
        :param dids: list of dicts {'scope': scope, 'name': name}
        :return: list of replicas
        """
        if self.client is not None:
            try:
                replicas = self.client.list_dataset_replicas_bulk(dids=dids)
            except Exception as e:
                replicas = []
                _logger.exception('Failed to get list of replicas:\n {}'.format(e))

            _logger.info('List of replicas got from Rucio for dids:\n {}\n {}'.format(dids, list(replicas)))

            return list(replicas)
        else:
            _logger.warning('Failed to initiate Rucio client, so it is impossible to get list of replicas for dids')
            return None


"""
    def getT1TapeSEForRR(self, RR = None, dataset = None):
        if not self.client is None:
            replicas = self.client.list_dataset_replicas(scope=None, name=dataset)
            for replica in replicas:
                print(replica)
                pass
"""



