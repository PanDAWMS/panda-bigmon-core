import logging
import re
from collections import Counter, defaultdict

_logger = logging.getLogger('bigpandamon')

class TasksErrorCodesAnalyser:
    def __init__(self):
        self._result = None

    future = None
    def is_float(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def is_int(self, value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    def is_percentage(self, value):
        regex = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*%\s*$', re.IGNORECASE)
        return bool(regex.match(value))

    def is_hours(self, value):
        if value[-1] == 'h' and self.is_int(value[:-1]):
            return True
        else:
            return False

    def is_volume(self, value):
        regex = re.compile(r'(\d+(?:\.\d+)?)\s*([kmgtp]?b)', re.IGNORECASE)
        if regex.match(value):
            return True
        else:
            return False

    def is_did(self, value):
        """Check if value is a DID, i.e. a file, dataset or container name"""
        regex = re.compile(r'.*(?:(mc|data|group|valid|user)\w*:)?(?:mc|data|group|valid|user)\w*(?:[\.\_\/]\w+)+', re.IGNORECASE)
        if regex.match(value):
            return True
        else:
            return False

    def is_vm_name(self, value):
        """Check if value is a VM hostname"""
        regex = re.compile(r'^aipanda\d+(?:\.\w+)*', re.IGNORECASE)
        if regex.match(value):
            return True
        else:
            return False

    def is_variable_token(self, w):
        return (self.is_vm_name(w) or self.is_did(w) or self.is_int(w) or self.is_float(w) or self.is_hours(w) or self.is_volume(w)
                or self.is_percentage(w))

    def remove_links(self, text):
        return re.sub(r'<a\b[^>]*>(.*?)<\/a>', '', text)

    def my_tokenizer(self, s):
        return list(filter(None, re.split("[/ \\-!?:()><=,\"]+", s)))

    def replace_all(self, text, words_to_replace=()):
        text = self.remove_links(text)
        common_tokens = set(self.my_tokenizer(text)).intersection(words_to_replace)
        common_tokens = sorted(common_tokens, key=len, reverse=True)
        for i in common_tokens:
            text = text.replace(f' {i}', ' *R*').replace(f'{i} ', '*R* ').replace(f'{i}', '*R*')
        return text

    def remove_special_character(self, input_str):
        bad_chars = ['"', "'", '\n']
        processed_string = ''.join('*' if ch in bad_chars else ch for ch in input_str)
        processed_string = re.sub(r'\*+', '*', processed_string)
        return processed_string

    def get_messages_groups(self, tasks_list):
        if not tasks_list:
            return []

        # unique message text -> set of task ids & sum total occurrences
        msg_to_tasks = defaultdict(set)
        msg_occurrences = Counter()
        for t in tasks_list:
            msg_to_tasks[t['errordialog']].add(t['jeditaskid'])
            msg_occurrences[t['errordialog']] += t.get('n', 1)

        # token frequencies across the corpus weighting each unique message by how many tasks have it
        word_freq = Counter()
        for msg, tids in msg_to_tasks.items():
            n = len(tids)
            for tok in self.my_tokenizer(msg):
                word_freq[tok] += n
        threshold = 1 if len(tasks_list) == 1 else 2
        words_to_replace = {w for w, c in word_freq.items()
                            if c < threshold or self.is_variable_token(w)}
        del word_freq

        # normalize each unique message once and group
        groups = {}
        for msg, tids in msg_to_tasks.items():
            processed = self.replace_all(msg, words_to_replace)
            g = groups.setdefault(processed, {'errordialog': msg, 'tids': set(), 'n': 0})
            g['tids'] |= tids
            g['n'] += msg_occurrences[msg]
        del msg_to_tasks

        # build output records
        result = []
        for processed, g in groups.items():
            errordialog = self.remove_special_character(g['errordialog'])
            if errordialog == '':
                continue
            tids = sorted(g['tids'])
            result.append({
                'processed_errordialog': self.remove_special_character(processed),
                'errordialog': errordialog,
                'count': g['n'],
                'n_tasks': len(tids),
                'link': 'jeditaskid=' + '|'.join(map(str, tids)),
            })
        result.sort(key=lambda r: r['count'], reverse=True)
        return result

    def process_error_messages(self, tasks_list):
        return self.get_messages_groups(tasks_list)

    def schedule_preprocessing(self, tasks_list):
        self._result = self.process_error_messages(tasks_list)

    def get_errors_table(self):
        return self._result if self._result is not None else []

