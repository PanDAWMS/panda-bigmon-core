import json, asyncio
import random

from datetime import datetime
from channels.generic.websocket import WebsocketConsumer, AsyncWebsocketConsumer
from core.kafka.config import initConsumer
from core.libs.elasticsearch import get_es_task_status_log
from core.kafka.utils import fixed_statuses, prepare_data_for_pie_chart, prepare_data_for_main_chart
class TaskLogsConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        #self.active_tasks_by_user = {}

        await self.accept()
        await self.send(text_data=json.dumps({
            'type': 'terminal', 'message': 'You have successfully connected to KAFKA! New messages for the tasks will appear automatically below'
        }))

        client = self.scope['client']
        user = self.scope['user']

        #self.active_tasks_by_user[user] = []

        jeditaskid = int(self.scope['url_route']['kwargs']['jeditaskid'])
        db_source = self.scope['url_route']['kwargs']['db_source']

        self.archived_messages, self.message_ids, self.jobs_info_status_dict = get_es_task_status_log(db_source=db_source,
                                                                                       jeditaskid=jeditaskid)
        if len(self.jobs_info_status_dict) > 1:
            hist_agg_data = await self.aggregated_data(self.jobs_info_status_dict)

            await self.send(text_data=json.dumps({
                'type': 'terminal',
                'message': hist_agg_data
            }))

        self.consumer = initConsumer(client, jeditaskid)
        # self.active_tasks_by_user[user].append(kafka_task)
        self.kafka_task = asyncio.create_task(self.kafka_consumer(jeditaskid))

    async def disconnect(self, close_code):
        if self.kafka_task and not self.kafka_task.done():
            self.kafka_task.cancel()
        self.consumer.close()

    async def kafka_consumer(self,  jeditaskid):
        loop = asyncio.get_running_loop()
        try:
            jobs_dict = {}

            for key, value in self.jobs_info_status_dict.items():
                jobs_dict[key] = max(value.values(), key=lambda item: item['timestamp'])
            jobs_list = list(jobs_dict.keys())

            await self.send(text_data=json.dumps({'type': 'jobs_list', 'message': jobs_list}))

            await self.send_metrics(jobs_dict)

            while True:
                await self.send_real_time_agg_data(None)
                message = await loop.run_in_executor(None, self.consumer.poll, 1.0)
                if message is None:
                    continue

                print('Received message: {}'.format(message.value().decode('utf-8')))

                message_dict = json.loads(message.value())

                is_message_meets_conditions = await self.message_filter(jeditaskid, message_dict)

                if (is_message_meets_conditions):
                    if message_dict['msg_type'] == 'task_status':
                        await self.send(text_data=json.dumps({'type': 'terminal', 'message': message.value()}))
                    else:
                        try:
                            jobs_dict[message_dict['jobid']] = message_dict
                            # sends metrics for plots
                            await self.send_real_time_plots_data(jobs_dict)
                            # await self.send_metrics(jobs_dict)
                            # sends messages to the terminal
                            agg_data = await self.aggregated_data(self.jobs_info_status_dict)
                            await self.send_real_time_agg_data(agg_data)
                        except Exception as ex:
                            print(ex)
        except Exception as ex:
            print(ex)

    async def websocket_receive(self, event):
        client_message = json.loads(event['text'])
        if client_message['type'] == 'get_job_history':
            jobid = int(client_message['pandaid'])
            job_history = self.jobs_info_status_dict[jobid]
            sorted_status_data = dict(sorted(job_history.items(), key=lambda x: x[1]['timestamp']))
            await self.send(text_data=json.dumps({'type': 'job_status_history', 'message': sorted_status_data}))
        if client_message['type'] == 'print_hist':
            #sending data to terminal
            for message in self.archived_messages:
                await self.send(text_data=json.dumps({'type': 'terminal', 'message': message}))

    async def message_filter(self, jeditaskid, message):
        is_message_meets_conditions = False
        if 'taskid' in message and message['taskid'] is not None:
            if int(jeditaskid) == int(message['taskid']):
                is_message_meets_conditions = True
        return is_message_meets_conditions

    async def send_metrics(self, jobs_dict):
        try:

            status_count = {status: 0 for status in fixed_statuses}

            metric_sum = {
                'job_hs06sec': {},
                'job_inputfilebytes': {},
                'job_nevents': {}
            }

            for key, value in jobs_dict.items():
                status = value['status']
                # ('failed, finished', 'cancelled', 'closed')
                if status in ('failed', 'finished', 'cancelled', 'closed'):
                    if status not in metric_sum['job_hs06sec']:
                        metric_sum['job_hs06sec'][status] = 0
                    if status not in metric_sum['job_inputfilebytes']:
                        metric_sum['job_inputfilebytes'][status] = 0
                    if status not in metric_sum['job_nevents']:
                        metric_sum['job_nevents'][status] = 0

                    if type(value['job_hs06sec']) is int:
                        metric_sum['job_hs06sec'][status] += value['job_hs06sec']
                    if type(value['job_inputfilebytes']) is int:
                        metric_sum['job_inputfilebytes'][status] += value['job_inputfilebytes']
                    if type(value['job_nevents']) is int:
                    	metric_sum['job_nevents'][status] += value['job_nevents']

                if value['status'] in status_count:
                    status_count[value['status']] += 1

            chart_js_statuses_dict = prepare_data_for_main_chart(status_count)
            chart_js_job_hs06sec = prepare_data_for_pie_chart(metric_sum['job_hs06sec'])
            chart_js_job_inputfilebytes = prepare_data_for_pie_chart(metric_sum['job_inputfilebytes'])
            chart_js_job_nevents = prepare_data_for_pie_chart(metric_sum['job_nevents'])

            await self.send(text_data=json.dumps({'type': 'metrics',
                                                  'chart_js_job_hs06sec':chart_js_job_hs06sec,
                                                  'chart_js_job_inputfilebytes': chart_js_job_inputfilebytes,
                                                  'chart_js_job_nevents': chart_js_job_nevents,
                                                  'chart_js_statuses_data': chart_js_statuses_dict}))
            return True
        except Exception as ex:
            print(ex)
            return False
    async def aggregated_data(self, jobs_dict):

        tmp_results = {}
        if len(jobs_dict) > 0:
            for key, value in jobs_dict.items():
                last_status = None

                for status, info in value.items():
                    if last_status is None or info['timestamp'] > value[last_status]['timestamp']:
                        last_status = status

                if last_status:
                    tmp_results[key] = last_status

            status_counts = {}
            for status in tmp_results.values():
                if status in status_counts:
                    status_counts[status] += 1
                else:
                    status_counts[status] = 1

            int_metrics_sum = {
                'job_hs06sec': 0,
                'job_inputfilebytes': 0,
                'job_nevents': 0
            }
            for value in jobs_dict.values():
                for status, info in value.items():
                    for metric in int_metrics_sum:
                        if metric in info:
                            int_metrics_sum[metric] += info[metric]

            time_list = [info['timestamp'] for value in jobs_dict.values() for info in value.values()]
            min_time = min(time_list)
            max_time = max(time_list)
            duration = max_time - min_time

            results = {
                'n_jobs': status_counts,
                'min_time': str(datetime.fromtimestamp(min_time)),
                'max_time': str(datetime.fromtimestamp(max_time)),
                'duration': duration,
                'metrics_sum': int_metrics_sum
            }
        else:
            results = {
                'n_jobs': None,
                'min_time': None,
                'max_time': None,
                'duration': None,
                'metrics_sum': None
            }
        return results

    async def send_real_time_agg_data(self, agg_data):
        await asyncio.sleep(30)
        await self.send(text_data=json.dumps({'type': 'terminal', 'message': agg_data}))

    async def send_real_time_plots_data(self, jobs_dict):
        await asyncio.sleep(10)
        await self.send_metrics(jobs_dict)
        await self.send(text_data=json.dumps({'type': 'jobs_list', 'message': list(jobs_dict.keys())}))

