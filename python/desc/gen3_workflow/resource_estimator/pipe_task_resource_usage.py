"""
Resource usage estimates for DRP pipe_tasks as run on a
Cori-Haswell node as of June 2020.   See
https://github.com/LSSTDESC/gen3_workflow/wiki/Resource-Usage-Testing-with-Gen2-pipe_tasks
"""
__all__ = ['pipe_tasks']


def nImages(num_visits):
    """
    Convert the number of visits to the mean number of images
    contributing to a given patch.  This is the average conversion
    ratio for a WFD survey candence.
    """
    return 0.77*num_visits


def processCcd():
    """
    Return the average cpu time in hours and required memory in GB for
    a single instance of processCcd.
    """
    return 2.5/60., 1.


def makeCoaddTempExp():
    """
    Return the average cpu time in hours and required memory in GB for
    a single instance of makeCoaddTempExp.
    """
    return 2.2/60., 1.6


def assembleCoadd(num_visits):
    """
    Return the average cpu time in hours and required memory in GB for
    a single instance of assembleCoadd, scaling with the number of visits.
    """
    return 0.4*num_visits/60., 1.5


def cpu_mem_visit_scaling(num_visits, cpu0, cpu_index, mem0, mem_index):
    """
    Return tuple of cpu time (hours), required memory (GB) for pipe tasks
    that operate on coadds.  Power-law scalings were fit to cpu_mins and
    mem_GB values found from DR6 studies on a Cori-Haswell node.
    """
    n_images = nImages(num_visits)
    cpu_mins = cpu0*n_images**cpu_index
    mem_GB = mem0*n_images**mem_index
    return cpu_mins/60., mem_GB


def detectCoaddSources(num_visits):
    """
    Return a tuple of cpu hours and required memory (in GB) for
    detectCoaddSources using the fit parameters found from DR6 studies.
    """
    return cpu_mem_visit_scaling(num_visits, 0.23, 0.78, 0.68, 0.26)


def mergeCoaddDetections():
    """
    Return a tuple of cpu hours and required memory (in GB) for
    mergeCoaddDetections.  These are the worst case values found
    from studies of DR6 data run on a Cori-Haswell node.
    """
    return 2.7/60., 0.7


def deblendCoaddSources(num_visits):
    """
    Return a tuple of cpu hours and required memory (in GB) for
    deblendCoaddSources using the fit parameters found from DR6 studies.
    """
    return cpu_mem_visit_scaling(num_visits, 0.16, 1.40, 0.30, 0.41)


def measureCoaddSources(num_visits):
    """
    Return a tuple of cpu hours and required memory (in GB) for
    measureCoaddSources using the fit parameters found from DR6 studies.
    """
    return cpu_mem_visit_scaling(num_visits, 1.80, 1.20, 0.43, 0.36)


def mergeCoaddMeasurements():
    """
    Return a tuple of cpu hours and required memory (in GB) for
    mergeCoaddMeasurements.  These are the worst case values found
    from studies of DR6 data run on a Cori-Haswell node.
    """
    return 1/60., 2.8


def forcedPhotCoadd(num_visits):
    """
    Return a tuple of cpu hours and required memory (in GB) for
    forcedPhotCoadd using the fit parameters found from DR6 studies.
    """
    return cpu_mem_visit_scaling(num_visits, 2.20, 1.20, 0.43, 0.36)


pipe_task_funcs = ['processCcd', 'makeCoaddTempExp', 'assembleCoadd',
                   'detectCoaddSources', 'mergeCoaddDetections',
                   'deblendCoaddSources', 'measureCoaddSources',
                   'mergeCoaddMeasurements', 'forcedPhotCoadd']


pipe_tasks = {_: eval(_) for _ in pipe_task_funcs}
