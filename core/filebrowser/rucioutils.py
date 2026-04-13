import logging
from core.filebrowser.ruciowrapper import ruciowrapper

_logger = logging.getLogger("bigpandamon-filebrowser")


def get_rucio_username_by_produserid(produserid, prodsourcelabel='user'):
    """
    Get Rucio username from DB
    :param produserid: str - production user id
    :param prodsourcelabel: str - production source label, default is 'user'
    :return: str - Rucio username
    """
    if prodsourcelabel == 'user':
        dn = produserid
        try:
            if produserid.startswith("/"):
                # OpenSSL -> RFC 2253 format & remove last part if it is a number (e.g. /CN=1234567890)
                produserid = produserid[1:]
                parts = produserid.split("/")
                if parts and parts[-1].startswith("CN="):
                    value = parts[-1][3:]
                    if value.isdigit():
                        parts.pop()
                parts.reverse()
                dn = ",".join(parts)
        except ValueError:
            _logger.exception(f"Failed to parse produserid: {produserid}")
            dn = produserid

        rw = ruciowrapper()
        rucio_username = rw.getRucioAccountByDN(dn)
        if len(rucio_username) > 1:
            rucio_username_unique = {}
            for un in rucio_username:
                if isinstance(un, dict):
                    if 'rucio_account' in un and un['rucio_account']:
                        rucio_username_unique[un['rucio_account']] = 1
                elif isinstance(un, str):
                    rucio_username_unique[un] = 1
            rucio_username = list(rucio_username_unique.keys())
    else:
        rucio_username = [produserid,]

    return rucio_username