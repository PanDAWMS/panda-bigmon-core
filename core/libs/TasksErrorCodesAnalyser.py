from sklearn.feature_extraction.text import CountVectorizer
import concurrent.futures
import logging
import pandas as pd
import urllib
import re

_logger = logging.getLogger('bigpandamon')

system_synonyms = ['aipanda052.cern.ch', 'aipanda058.cern.ch', 'aipanda054.cern.ch','aipanda189.cern.ch',
                   'aipanda055.cern.ch']

class TasksErrorCodesAnalyser:
    future = None

    def isfloat(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False


    def isint(self, value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    def isHours(self, value):
        if value[-1] == 'h' and self.isint(value[:-1]):
            return True
        else:
            return False

    def remove_stop_words(self, frame):
        def my_tokenizer(s):
            return list(filter(None, re.split("[/ \\-!?:()><=,]+", s)))

        vectorizer = CountVectorizer(tokenizer=my_tokenizer, analyzer="word", stop_words=None, preprocessor=None)
        corpus = frame['errordialog'].tolist()
        try:
            bag_of_words = vectorizer.fit_transform(corpus)
            sum_words = bag_of_words.sum(axis=0)
            words_freq = [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()]
            words_freqf = [x[0] for x in filter(lambda x: (x[1] < 2 or
                                                          self.isint(x[0]) or self.isfloat(x[0]) or x[0] in
                                                          system_synonyms or self.isHours(x[0])), words_freq)]
            words_freqf = set(words_freqf)

            def replace_all(text):
                common_tokens = set(my_tokenizer(text)).intersection(words_freqf)
                common_tokens = sorted(common_tokens, key=len, reverse=True)
                for i in common_tokens:
                    text = text.replace(i, '*R*')
                return text
            frame['processed_errordialog'] = frame['errordialog'].apply(replace_all)
        except:
            frame['processed_errordialog'] = frame['errordialog']
        return frame

    def encode_into_link(self, error_msg):
        error_dialog = "'{message}'".format(message=error_msg).replace("**REPLACEMENT**", "*")
        error_dialog = 'errordialog='+urllib.parse.quote(error_dialog, safe='')
        return error_dialog

    def remove_special_character(self, input_str):
        bad_chars = ['"', ':', "'", '\n']
        processed_string = ''.join((filter(lambda i: i not in bad_chars, input_str)))
        return processed_string

    def get_messages_groups(self, tasks_list):
        tasks_errors_groups = []
        if len(tasks_list) > 0:
            frame_rows = list(map(lambda z: (z['jeditaskid'], z['errordialog'], ""), tasks_list))
            tasks_errors_frame = pd.DataFrame(frame_rows, columns=['taskid','errordialog','processed_errordialog'])
            tasks_errors_frame = self.remove_stop_words(tasks_errors_frame)
            tasks_errors_groups = tasks_errors_frame.groupby('processed_errordialog').\
                agg({'taskid': lambda x: list(x), 'errordialog': 'first'}).reset_index()
            if tasks_errors_groups.empty:
                return []
            tasks_errors_groups['count'] = tasks_errors_groups.apply(lambda row: len(row['taskid']), axis=1)
            tasks_errors_groups = tasks_errors_groups.sort_values(by=['count'], ascending=False)

            # This step is needed due to issues with JSON encoding when deliver to template
            tasks_errors_groups['errordialog'] = tasks_errors_groups.\
                apply(lambda row: self.remove_special_character(row['errordialog']), axis=1)
            tasks_errors_groups['processed_errordialog'] = tasks_errors_groups.\
                apply(lambda row: self.remove_special_character(row['processed_errordialog']), axis=1)

            self.add_site_information(tasks_list, tasks_errors_groups)
            tasks_errors_groups['link'] = tasks_errors_groups.apply(lambda row: 'jeditaskid=' + '|'.join([str(taskid) for taskid in row['taskid']]), axis=1)

            del tasks_errors_groups['taskid']
            tasks_errors_groups.drop(tasks_errors_groups[tasks_errors_groups.errordialog == ''].index, inplace=True)
            tasks_errors_groups = tasks_errors_groups.to_dict('records')
        return tasks_errors_groups

    def add_site_information(self, tasks_list, tasks_errors_groups):
        tasks_to_site = {t['jeditaskid']: t['nucleus'] for t in tasks_list if 'nucleus' in t and t['nucleus']}
        tasks_errors_groups['sites'] = tasks_errors_groups.apply(lambda row: ', '.join(set([tasks_to_site.get(task,'')
                                                                              for task in row['taskid']])), axis=1)

    def process_error_messages(self, tasks_list):
        return self.get_messages_groups(tasks_list)

    def schedule_preprocessing(self, tasks_list):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            self.future = executor.submit(self.process_error_messages, tasks_list)

    def get_errors_table(self):
        if self.future != None:
            return_value = self.future.result()
        return return_value

