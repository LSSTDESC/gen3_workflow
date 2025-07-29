#!/usr/bin/env python
from desc.gen3_workflow import start_pipeline
graph = start_pipeline('bps_cpBias.yaml')
graph.shutdown()
