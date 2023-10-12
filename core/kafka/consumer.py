import json, asyncio

from channels.generic.websocket import WebsocketConsumer, AsyncWebsocketConsumer
from core.kafka.config import initConsumer
from core.libs.elasticsearch import get_es_task_status_log


class TaskLogsConsumer(AsyncWebsocketConsumer):
    fixed_statuses = ['pending', 'defined', 'waiting', 'assigned', 'throttled',
                      'activated', 'sent', 'starting', 'running', 'holding',
                      'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']

    status_colors = {
        'pending': 'rgba(222, 185, 0, 1)',
        'defined': 'rgba(33, 116, 187, 1)',
        'waiting': 'rgba(222, 185, 0, 1)',
        'assigned': 'rgba(9, 153, 153, 1)',
        'throttled': 'rgba(255, 153, 51, 1)',
        'activated': 'rgba(59, 142, 103, 1)',
        'sent': 'rgba(222, 185, 0, 1)',
        'starting': 'rgba(47, 209, 71, 1)',
        'running': 'rgba(52, 169, 52, 1)',
        'holding': 'rgba(255, 153, 51, 1)',
        'transferring': 'rgba(52, 169, 52, 1)',
        'merging': 'rgba(52, 169, 52, 1)',
        'finished': 'rgba(32, 127, 32, 1)',
        'failed': 'rgba(255, 0, 0, 1)',
        'cancelled': 'rgba(230, 115, 0, 1)',
        'closed': 'rgba(74, 74, 74, 1)'
    }
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

        await self.send(text_data=json.dumps({
            'type': 'terminal', 'message': f'Information about this task contains {len(self.message_ids)} entries in ElasticSearch storage. To display them please print "hist" command'
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

            await self.send(text_data=json.dumps({'type': 'jobs_list', 'message': jobs_list }))

            await self.send_metrics(jobs_dict)

            while True:
                message = await loop.run_in_executor(None, self.consumer.poll, 1.0)
                if message is None:
                    # Dynamic testing
                    # import random
                    # statuses = ['pending', 'defined', 'waiting', 'assigned', 'throttled',
                    #             'activated', 'sent', 'starting', 'running', 'holding',
                    #             'transferring', 'merging', 'finished', 'failed', 'cancelled', 'closed']
                    #
                    #
                    # random_key = random.randint(10000000, 99999999)
                    #
                    # random_status = random.choice(statuses)
                    #
                    # random_job_hs06sec = random.randint(0, 100)
                    # random_job_inputfilebytes = random.randint(0, 10000)
                    # random_job_nevents = random.randint(0, 1000)
                    # jobs_dict[random_key] = {'job_hs06sec': 0, 'job_inputfilebytes': 0, 'job_nevents': 0,
                    #                        'message_id': 'd699e53ea941a4f116eefb1900d9a2eda49b3c91',
                    #                        'status': random_status, 'timestamp': 1696942835}
                    # await self.send_metrics(jobs_dict)
                    continue


                message_dict = json.loads(message.value())

                is_message_meets_conditions = await self.message_filter(jeditaskid, message_dict)

                if (is_message_meets_conditions):
                    jobs_dict[message_dict['jobid']] = message_dict
                    # sends metrics for plots
                    await self.send_metrics(jobs_dict)
                    # sends messages to the terminal
                    await self.send(text_data=json.dumps({'type': 'terminal', 'message': message_dict}))

                    await self.send(text_data=json.dumps({'type': 'jobs_list', 'message': jobs_dict.keys()}))

                print('Received message: {}'.format(message.value().decode('utf-8')))

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

            status_count = {status: 0 for status in self.fixed_statuses}

            metric_sum = {
                'job_inputfilebytes': 0,
                'job_hs06sec': 0,
                'job_nevents': 0
            }

            for key, value in jobs_dict.items():
                if value['status'] in ('failed, finished', 'cancelled', 'closed'):
                    metric_sum['job_inputfilebytes'] += value['job_inputfilebytes']
                    metric_sum['job_hs06sec'] += value['job_hs06sec']
                    metric_sum['job_nevents'] += value['job_nevents']

                if value['status'] in status_count:
                    status_count[value['status']] += 1

            chart_js_dict = {
                'labels':  self.fixed_statuses,
                'datasets': [
                    {
                        'data': [status_count[status] for status in  self.fixed_statuses],
                        'backgroundColor': [self.status_colors[status] for status in self.fixed_statuses],
                        'borderColor': [self.status_colors[status] for status in self.fixed_statuses],
                    }
                ]
            }
            await self.send(text_data=json.dumps({'type': 'metrics', 'metric_sum': metric_sum,
                                                  'statuses_data': chart_js_dict}))
            return True
        except Exception as ex:
            print(ex)
            return False