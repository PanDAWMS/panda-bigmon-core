
from core.pandajob.models import Jobsarchived4, Jobsarchived, Jobsactive4, Jobsdefined4, Jobswaiting4
from core.libs.job import add_job_category, drop_duplicates
import core.constants as const


class UserProfilePlot:

    def __init__(self):
        pass

    def get_raw_data_profile_full(self, query):
        """
        A method to form a non ES task profile
        :param username:
        :return:
        """
        jobs = []
        # use modifacation time as it is DB index, also cut by creationdate
        query['creationtime__gte'] = query['modificationtime__castdate__range'][0]

        jvalues = ('pandaid', 'processingtype', 'transformation', 'jobstatus', 'starttime', 'creationtime', 'endtime',)

        jobs.extend(Jobsarchived4.objects.filter(**query).values(*jvalues))
        jobs.extend(Jobsactive4.objects.filter(**query).values(*jvalues))
        jobs.extend(Jobswaiting4.objects.filter(**query).values(*jvalues))
        jobs.extend(Jobsdefined4.objects.filter(**query).values(*jvalues))

        jobs.extend(Jobsarchived.objects.filter(**query).values(*jvalues))

        jobs = drop_duplicates(jobs)
        jobs = add_job_category(jobs)
        jobs = sorted(jobs, key=lambda x: x['creationtime'])

        # overwrite nevents to 0 for unfinished and build/merge jobs
        job_final_states = ['finished', 'closed', 'failed', 'cancelled']
        job_categories = ['build', 'run', 'merge']
        job_timestamps = ['creation', 'start', 'end']

        # create task profile dict
        user_Dataprofile_dict = {}
        for jc in job_categories:
            user_Dataprofile_dict[jc] = []
        fin_i = 0
        for j in jobs:
            fin_i += 1
            temp = {}
            for jtm in job_timestamps:
                temp[jtm] = j[jtm+'time']
            temp['indx'] = fin_i
            temp['jobstatus'] = j['jobstatus'] if j['jobstatus'] in job_final_states else 'active'
            temp['pandaid'] = j['pandaid']
            user_Dataprofile_dict[j['category']].append(temp)

        return user_Dataprofile_dict

