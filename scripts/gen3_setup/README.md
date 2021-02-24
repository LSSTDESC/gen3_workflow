# Converting a Gen2 repo to Gen3 (raw data only)

The `convert_gen2-repo.py` script will do this for Gen2 repos which have  CALIB folders that have been updated to be consistent with `lsst_distrib` versions as recent as `w_2020_49`, for which the master flats need to have physical filter names included in their paths.

To convert a repo with this script, one must create the Gen3 repo first with
```
$ butler create [--seed-config <butler config>] <Gen3 repo>
```
Here an optional seed config file can be provided if one wants to use a Postgres database for the registry, otherwise an sqlite3 registry will be created.

Running
```
$ python convert_gen2-repo.py <Gen2 repo> <Gen3 repo>
```
will ingest the raw files, the CALIB files, and the reference catalog files, making symlinks to the Gen2 versions.  It will also register the DC2 sky map and will set a default collection `LSSTCam-imSim/defaults` that chains together all of these data and which can be used as the input collection to a `pipetask` call.
