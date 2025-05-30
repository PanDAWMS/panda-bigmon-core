from core.filebrowser.ruciowrapper import ruciowrapper


def get_rucio_username_by_produserid(produserid, prodsourcelabel='user'):
    """
    Get Rucio username from DB
    :param produserid: str - production user id
    :param prodsourcelabel: str - production source label, default is 'user'
    :return: str - Rucio username
    """
    if prodsourcelabel == 'user':
        try:
            CNs = produserid.split("/CN=")
            if len(CNs) > 1:
                int(CNs[-1])
                produserid = produserid[:-(len(CNs[-1]) + 4)]
        except ValueError:
            pass
        rw = ruciowrapper()
        rucio_username = rw.getRucioAccountByDN(produserid)
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