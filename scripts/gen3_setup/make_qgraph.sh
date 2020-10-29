tract=3828
patch=24
pipeline=DRP

pipetask qgraph \
   -d "tract=${tract} AND patch=${patch}" \
   -i "LSSTCam-imSim/raw/all,LSSTCam-imSim/calib,ref_cat,skymaps/imsim" \
   -b gen3-repo \
   --instrument lsst.obs.lsst.LsstCamImSim \
   --pipeline ${GEN3_WORKFLOW_DIR}/pipelines/${pipeline}.yaml \
   --save-qgraph ${pipeline}_${tract}_${patch}.pickle \
   --qgraph-dot ${pipeline}_${tract}_${patch}.dot

#   -d "abstract_filter='i' and tract=3828 and patch=24 and detector=177" \
