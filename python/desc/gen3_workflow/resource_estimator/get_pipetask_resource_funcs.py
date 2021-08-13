"""
Resource usage estimates for Gen3 DRP pipetasks as run on a
Cori-Haswell node as of August 2021
https://github.com/LSSTDESC/gen3_workflow/wiki/Gen3--Investigations#2021-06-08-profiling-of-gen3-tasks-on-cori-haswell
"""
import json
import numpy as np

__all__ = ['get_pipetask_resource_funcs']

class PipetaskFunc:
    def __init__(self, cpu_time, maxRSS):
        self.cpu_time = cpu_time
        self.maxRSS = maxRSS

    def __call__(self, num_visits=0):
        return self.cpu_time(num_visits), self.maxRSS(num_visits)

def get_pipetask_resource_funcs(json_file, cpu_time_label='cpu_time (m)',
                                maxRSS_label='maxRSS (GB)',
                                cpu_time_factor=1./60., maxRSS_factor=1):
    with open(json_file) as fd:
        model_params = json.load(fd)
    pipetask_funcs = dict()
    for task, pars in model_params.items():
        cpu_time = np.poly1d(cpu_time_factor*np.array(pars[cpu_time_label]))
        maxRSS = np.poly1d(maxRSS_factor*np.array(pars[maxRSS_label]))
        pipetask_funcs[task] = PipetaskFunc(cpu_time, maxRSS)
    return pipetask_funcs

if __name__ == '__main__':
    json_file = 'resource_params_3828-y1.json'
    pipetask_funcs = get_pipetask_resource_funcs(json_file)
    num_visits = 20
    for task, func in pipetask_funcs.items():
        print(task, func(num_visits))
