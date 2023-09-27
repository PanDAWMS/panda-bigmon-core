import json
import asyncio, time

from channels.generic.websocket import WebsocketConsumer, AsyncWebsocketConsumer
from core.kafka.config import initConsumer
from core.libs.elasticsearch import get_es_task_status_log


class TaskLogsConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        await self.send(text_data=json.dumps({
            'type': 'terminal', 'message': 'You have successfully connected to KAFKA!'
        }))

        client = self.scope['client']
        jeditaskid = int(self.scope['url_route']['kwargs']['jeditaskid'])
        db_source = self.scope['url_route']['kwargs']['db_source']

        self.consumer = initConsumer(client)
        self.kafka_task = asyncio.create_task(self.kafka_consumer(db_source, jeditaskid))

    async def disconnect(self, close_code):
        self.consumer.close()

    async def kafka_consumer(self, db_source, jeditaskid):
        loop = asyncio.get_running_loop()
        try:
            jobs_dict = {}
            archived_messages, message_ids, jobs_info_status_dict = get_es_task_status_log(db_source=db_source, jeditaskid=jeditaskid)

            for key, value in jobs_info_status_dict.items():
                jobs_dict[key] = max(value.values(), key=lambda item: item['timestamp'])

            is_successful = await self.send_metrics(jobs_dict)

            for message in archived_messages:
                await self.send(text_data=json.dumps({'type': 'terminal', 'message': message}))

            while True:

                message = await loop.run_in_executor(None, self.consumer.poll, 1.0)
                if message is None:
                    continue

                message_dict = json.loads(message.value())
                await self.send(text_data=json.dumps({'type': 'terminal','message': message_dict}))
                print('Received message: {}'.format(message.value().decode('utf-8')))

                jobs_dict[message_dict['panda_id']] = message_dict
                is_successful = await self.send_metrics(jobs_dict)

        except Exception as ex:
            print(ex)

    async def message_filter(self, message):
        filtered_message = ''

        return filtered_message

    async def send_metrics(self, jobs_dict):
        try:
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

            status_count = {status: 0 for status in fixed_statuses}

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
                'labels': fixed_statuses,
                'datasets': [
                    {
                        'data': [status_count[status] for status in fixed_statuses],
                        'backgroundColor': [status_colors[status] for status in fixed_statuses],
                        'borderColor': [status_colors[status] for status in fixed_statuses],
                    }
                ]
            }
            await self.send(text_data=json.dumps({'type': 'metrics', 'metric_sum': metric_sum,
                                                  'statuses_data': chart_js_dict}))
            return True
        except Exception as ex:
            print(ex)
            return False