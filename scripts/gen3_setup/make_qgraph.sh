#pipeline=ProcessCcd
#pipeline=Coaddition
pipeline=DRP

pipetask qgraph \
   -d "tract=3828 AND patch=24" \
   -i "LSST-ImSim/raw/all,LSST-ImSim/calib,refcats,skymaps/imsim" \
   -b gen3-repo/butler.yaml \
   --skip-existing \
   --instrument lsst.obs.lsst.LsstCamImSim \
   --pipeline ${GEN3_WORKFLOW_DIR}/pipelines/${pipeline}.yaml \
   --save-qgraph ${pipeline}.pickle \
   --qgraph-dot ${pipeline}.dot

#   -d "abstract_filter='i' and tract=3828 and patch=24 and detector=177" \
