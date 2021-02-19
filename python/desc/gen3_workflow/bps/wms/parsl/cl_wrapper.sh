#!/bin/bash
#
# This wrapper script could be used with a shifter image to wrap a
# command line like this:
#
# # shifter --image=lsstsqre/centos:7-stack-lsst_distrib-w_2021_08 ${GEN3_WORKFLOW_DIR}/python/desc/gen3_workflow/bps/wms/parsl/cl_wrapper.sh <command line>
#
home=/global/u1/j/jchiang8
stack_dir=/opt/lsst/software/stack
dev_dir=/global/u1/j/jchiang8/dev
source ${stack_dir}/loadLSST.bash
setup lsst_distrib
setup -r ${dev_dir}/gen3_workflow -j
setup -r ${dev_dir}/daf_butler -j
setup -r ${dev_dir}/ctrl_bps -j
export PATH=${home}/bin:${home}/.local/bin:${PATH}
export OMP_NUM_THREADS=1

$*
exit $?
