"""
Parse and analyze task submission options
"""
import shlex
import logging

_logger = logging.getLogger('bigpandamon')

def parse_submission_command(comm):
    """
    Parse task submission command into dict
    :param comm: str - prun or pathena command
    :return: options: dict - {option_name: option_value}
    """
    options = {}

    try:
        cli_params_split = shlex.split(comm)
    except Exception as ex:
        _logger.exception(f'Failed to parse task submission command, return empty dict. {ex}')
        cli_params_split = []

    for i, cp in enumerate(cli_params_split[1:]):
        if cp.startswith('--'):
            cp_ = cp.replace('--', '')
            if '=' in cp_:
                k = cp_.split('=')[0]
                v = cp_.split('=')[1]
            elif i < len(cli_params_split) - 2 and not cli_params_split[1 + i + 1].startswith('--'):
                k = cp_
                v = cli_params_split[1 + i + 1]
            else:
                k = cp_
                v = None

            try:
                v = float(v)
            except:
                pass
            options[k] = v

    return options


def analyse_task_submission_options(cli_params):
    """
    Analyse options used in submission command and return warnings which will be shown to a user
    :param cli_params: str - prun or pathena command
    :return: warnings: dict - warnings to show to a user
    """

    options = parse_submission_command(cli_params)
    warnings = {}
    warning_desc = {
        'memory': {
            'high_memory': {
                'condition': 'more',
                'value_threshold': 4000,
                'message': (
                    "<b>{}</b> MB/core was requested for this task, "
                    "which severely restricts the available resources to run on. " 
                    "This task will take longer or may not run at all. "
                    "Check if it is really needed, and maybe improve the code."),
            }
        }
    }

    options_to_check = list(set(options.keys()) & set(warning_desc.keys()))
    if len(options_to_check) > 0:
        for o in options_to_check:
            for w, desc in warning_desc[o].items():
                if desc['condition'] == 'more':
                    if options[o] > desc['value_threshold']:
                        warnings[w] = desc['message'].format(options[o])
                elif desc['condition'] == 'less':
                    if options[o] < desc['value_threshold']:
                        warnings[w] = desc['message'].format(options[o])
                elif desc['condition'] == 'equals':
                    if options[o] == desc['value_threshold']:
                        warnings[w] = desc['message'].format(options[o])


    return warnings