__all__ = ['create_process_dag']

class Dag:
    def __init__(self):
        self.edges = set()

    def process(self, jobs):
        prereqs = []
        task_types = set()
        for job in jobs:
            for prereq in job.prereqs:
                self.edges.add(f'{prereq.gwf_job.label}->{job.gwf_job.label}')
                if prereq.gwf_job.label in task_types:
                    continue
                task_types.add(prereq.gwf_job.label)
                prereqs.append(prereq)
        if prereqs:
            self.process(prereqs)

    def write_dotfile(self, outfile):
        with open(outfile, 'w') as fd:
            fd.write('digraph DAG {\n')
            for edge in self.edges:
                fd.write(edge + ';\n')
            fd.write('}\n')

def create_process_dag(parsl_graph, outfile=None):
    # Find end-point jobs, limiting to unique task types
    end_points = set()
    task_types = set()
    for job in parsl_graph.values():
        if job.dependencies:
            continue
        if job.gwf_job.label not in task_types:
           task_types.add(job.gwf_job.label)
           end_points.add(job)

    dag = Dag()
    dag.process(end_points)

    if outfile is not None:
        dag.write_dotfile(outfile)

    return dag
