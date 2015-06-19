"""
    filebrowser.tests_data
"""

TESTS_DATA = {
    'cvmfs_path': '/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase',
    'test_file_exists': {
        'lfn': 'user.tbendall.ZTauTau.version33.1909.log.4165042.000101.log.tgz',
        'scope': 'user.tbendall',
        'guid':'722d7bb3-3c03-4d1f-ae1a-584924170710',
        'site': 'ANALY_DESY-HH',
    },
    'test_file_failure': {
        'lfn': 'user.jschovan.test-fake-log.000101.log.tgz',
        'scope': 'user.jschovan',
        'guid':'jschovan-3c03-4d1fi-ae1a-5840jschovan',
        'site': 'ANALY_FZU',
    },
}
# ?guid=722d7bb3-3c03-4d1f-ae1a-584924170710&lfn=user.tbendall.ZTauTau.version33.1909.log.4165042.000101.log.tgz&site=ANALY_DESY-HH&scope=user.tbendall
