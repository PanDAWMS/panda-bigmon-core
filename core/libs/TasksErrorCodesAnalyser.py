from sklearn.feature_extraction.text import CountVectorizer
import concurrent.futures
import logging
import pandas as pd
import urllib

_logger = logging.getLogger('bigpandamon')


class TasksErrorCodesAnalyser:
    future = None

    def remove_stop_words(self, frame):
        def my_tokenizer(s):
            return s.split()
        vectorizer = CountVectorizer(tokenizer=my_tokenizer, analyzer="word", stop_words=None, preprocessor=None)
        corpus = frame['errordialog'].tolist()
        bag_of_words = vectorizer.fit_transform(corpus)
        sum_words = bag_of_words.sum(axis=0)
        words_freq = [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()]
        words_freqf = [x[0] for x in filter(lambda x: x[1] < 3, words_freq)]
        words_freqf = set(words_freqf)
        def replace_all(text):
            common_tokens = set(my_tokenizer(text)).intersection(words_freqf)
            for i in common_tokens:
                text = text.replace(i, '**REPLACEMENT**') if len(i) > 3 else text
            return text
        frame['processed_errordialog'] = frame['errordialog'].apply(replace_all)
        return frame

    def encode_into_link(self, error_msg):
        error_dialog = "'{message}'".format(message=error_msg).replace("**REPLACEMENT**", "*")
        error_dialog = 'errordialog='+urllib.parse.quote(error_dialog, safe='')
        return error_dialog

    def remove_special_character(self, input_str):
        bad_chars = ['"', ':', "'"]
        processed_string = ''.join((filter(lambda i: i not in bad_chars, input_str)))
        return processed_string

    def get_messages_groups(self, tasks_list):
        if len(tasks_list) > 0:
            frame_rows = list(map(lambda z: (z['jeditaskid'], z['errordialog'], ""), tasks_list))
            tasks_errors_frame = pd.DataFrame(frame_rows, columns=['taskid','errordialog','processed_errordialog'])
            tasks_errors_frame = self.remove_stop_words(tasks_errors_frame)
            tasks_errors_groups = tasks_errors_frame.groupby('processed_errordialog').\
                agg({'taskid': lambda x: list(x), 'errordialog': 'first'}).reset_index()
            tasks_errors_groups['count'] = tasks_errors_groups.apply(lambda row: len(row['taskid']), axis=1)
            tasks_errors_groups['link'] = tasks_errors_groups.\
                apply(lambda row: self.encode_into_link(row['processed_errordialog']), axis=1)
            tasks_errors_groups = tasks_errors_groups.sort_values(by=['count'], ascending=False)

            # This step is needed due to issues with JSON encoding when deliver to template
            tasks_errors_groups['errordialog'] = tasks_errors_groups.\
                apply(lambda row: self.remove_special_character(row['errordialog']), axis=1)

            del tasks_errors_groups['taskid']
        return tasks_errors_groups.to_dict('records')

    def process_error_messages(self, tasks_list):
        return self.get_messages_groups(tasks_list)

    def schedule_preprocessing(self, tasks_list):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            self.future = executor.submit(self.process_error_messages, tasks_list)

    def get_errors_table(self):
        if self.future != None:
            return_value = self.future.result()
        return return_value

