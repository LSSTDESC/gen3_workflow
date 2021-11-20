#!/bin/bash
set -x

repo=test_repo
instrument=lsst.obs.lsst.LsstCam

butler create ${repo}
butler register-instrument ${repo} ${instrument}
butler write-curated-calibrations ${repo} ${instrument}
butler ingest-raws ${repo} raw_data/*.fits.fz

bps submit bps_cpBias.yaml
