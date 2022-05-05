"""
A set of functions to reconstruct job chains based on parentid field in JOBS* tables
"""


def reconstructJobsConsumersHelper(chainsDict):
    reconstructionDict = {}
    modified = False
    for pandaid, parentids in chainsDict.items():
        if parentids and parentids[-1] in chainsDict:
            if chainsDict[parentids[-1]]:
                reconstructionDict[pandaid] = parentids + chainsDict[parentids[-1]]
                modified = True
            else:
                reconstructionDict[pandaid] = parentids
        else:
            reconstructionDict[pandaid] = parentids

    if modified:
        return reconstructJobsConsumersHelper(reconstructionDict)
    else:
        return reconstructionDict


def reconstruct_job_consumers(jobsList):

    jobsInheritance = {}

    # Fill out all possible consumers
    for job in jobsList:
        jobsInheritance[job['pandaid']] = [job['parentid']]

    chains = reconstructJobsConsumersHelper(jobsInheritance)
    cleanChain = {}
    for name, value in chains.items():
        if len(value) > 1:
            cleanChain[name] = value[-2]
            for pandaid in value[:-2]:
                cleanChain[pandaid] = value[-2]

    for job in jobsList:
        if job['pandaid'] in cleanChain:
            job['consumer'] = cleanChain[job['pandaid']]
        else:
            job['consumer'] = None

    return jobsList