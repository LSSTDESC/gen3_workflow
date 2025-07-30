#!/bin/bash
set -x
if [ -z "$@" ]
then
    payload="bps submit bps_cpBias.yaml"
else
    payload=$@
fi

repo=test_repo
instrument=lsst.obs.lsst.LsstCam

export BUTLER_CONFIG=`pwd -P`/${repo}

butler create ${repo}
butler register-instrument ${repo} ${instrument}
butler write-curated-calibrations --collection LSSTCam/calib ${repo} ${instrument}
butler ingest-raws ${repo} ${GEN3_WORKFLOW_DIR}/tests/cpBias_test/raw_data/*.fits.fz

${payload}
