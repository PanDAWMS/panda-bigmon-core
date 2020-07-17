from rucio.client import Client, downloadclient
import logging
from core.common.models import RucioAccounts
from django.utils import timezone
from datetime import datetime, timedelta
import os
from .utils import get_rucio_account, get_x509_proxy
from core.filebrowser.utils import get_fullpath_filebrowser_directory, get_filebrowser_directory
import uuid

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
            basedir = get_filebrowser_directory() +'/'+ str(uuid.uuid4())+'/'
            dclient.download_dids([{'did':ds_name,
                                    'base_dir':basedir}])
        except Exception as e:
            logging.error('Failed to get accounts' + str(e))
            return {'exception':str(e)}
        return {'exception':None, 'basedir':basedir}

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

"""
    def getT1TapeSEForRR(self, RR = None, dataset = None):
        if not self.client is None:
            replicas = self.client.list_dataset_replicas(scope=None, name=dataset)
            for replica in replicas:
                print(replica)
                pass
"""



