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

errors_diag_fields_list = ['brokerageerrordiag', 'ddmerrordiag', 'exeerrordiag', 'jobdispatchererrordiag',
                           'piloterrordiag', 'superrordiag', 'taskbuffererrordiag']

def prepare_data_for_main_chart(data):
    labels = fixed_statuses
    values = [data[status] for status in fixed_statuses]

    return {
        'labels': labels,
        'datasets': [
            {
                'data': values,
                'backgroundColor': [status_colors[status] for status in fixed_statuses],
            }
        ]
    }
def prepare_data_for_pie_chart(data):
    labels = list(data.keys())
    values = [data[label] for label in labels]
    return {
        'labels': labels,
        'datasets': [
            {
                'data': values,
                'backgroundColor': [status_colors[label] for label in labels],
                'borderColor': [status_colors[label] for label in labels],
            }
        ]
    }
def update_errors_list(job, jobs_info_errors_dict):
    jobs_info_errors_dict[job['jobid']] = {}
    jobs_info_errors_dict[job['jobid']]['errors'] = []
    for field in errors_diag_fields_list:
        if job[field] != 'NULL':
            error_field = field.replace("diag", "")
            error_code_field = field.replace("diag", "code")

            jobs_info_errors_dict[job['jobid']]['errors'].append({
                'timestamp': job['@timestamp'],
                'error_type': error_field,
                'code': job.get(error_code_field, "None"),
                'text': job.get(field, "None")
            })
    return jobs_info_errors_dict

def create_new_errors_list(job):
    jobs_info_errors_dict = {}
    jobs_info_errors_dict[job['jobid']] = {}
    jobs_info_errors_dict[job['jobid']]['errors'] = []
    for field in errors_diag_fields_list:
        if job[field] != 'NULL':
            error_field = field.replace("diag", "")
            error_code_field = field.replace("diag", "code")

            jobs_info_errors_dict[job['jobid']]['errors'].append({
                'timestamp': job['@timestamp'],
                'error_type': error_field,
                'code': job.get(error_code_field, "None"),
                'text': job.get(field, "None")
            })

    return jobs_info_errors_dict