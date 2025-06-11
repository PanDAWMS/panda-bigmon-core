import logging
import re

from core.art.utils import get_test_results as get_test_results_art
import core.buildmonitor.constants as const
from django.core.cache import cache

_logger = logging.getLogger("bigpandamon")


def get_art_test_results():
    """
    Getting ART test results from cache, if they are not there - get from DB and put in cache for further use in other views.
    :return: art_test_results
    """

    art_test_results = cache.get("art_results")
    if art_test_results is None:
        try:
            art_test_results_new = get_test_results_art(const.N_DAYS_ART_RESULTS, test_type="all", agg_by="branch")
        except ValueError:
            _logger.exception("Failed to get ART test results")
            return None
        except Exception as e:
            _logger.exception("General Error\n{}".format(str(e)))
            return None

        if len(art_test_results_new) > 0:
            # rename branch names to match the ones in ALTR DB (/ -> _) and add ntag to the end of the key instead of nesting dict
            art_test_results = {}
            for k, v in art_test_results_new.items():
                for ntag, stats in v.items():
                    art_test_results[f'{re.sub("/", "_", k)}_{ntag}'] = stats
            cache.set("art_results", art_test_results, const.CACHE_TIMEOUT_MINUTES_ART_RESULTS * 60)

    return art_test_results
