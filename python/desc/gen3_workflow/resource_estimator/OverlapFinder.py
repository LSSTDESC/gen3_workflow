"""
Module to compute overlaps of patches with ccd-visits.
"""
import os
import sys
from collections import defaultdict
import pickle
import sqlite3
import numpy as np
import pandas as pd
import lsst.geom
import lsst.sphgeom
import lsst.log
from lsst.afw.cameraGeom import DetectorType
from lsst.obs.base.utils import createInitialSkyWcsFromBoresight
from lsst.obs.lsst import LsstCam

lsst.log.setLevel('', lsst.log.ERROR)


__all__ = ['SkyMapPolygons', 'OverlapFinder', 'extract_coadds',
           'unique_tuples']


LSSTCAM = LsstCam.getCamera()

class SkyMapPolygons:

    @staticmethod
    def makeBoxWcsRegion(box, wcs, margin=0.0):
        """Construct a spherical ConvexPolygon from a WCS and a bounding box.
        Parameters
        ----------
        box : lsst.geom.Box2I or lsst.geom.Box2D
            A box in the pixel coordinate system defined by the WCS.
        wcs : afw.image.Wcs
            A mapping from a pixel coordinate system to the sky.
        margin : float
            A buffer in pixels to grow the box by (in all directions) before
            transforming it to sky coordinates.

        Returns
        -------
        lsst.sphgeom.ConvexPolygon
        """
        box = lsst.geom.Box2D(box)
        box.grow(margin)
        vertices = []
        for point in box.getCorners():
            coord = wcs.pixelToSky(point)
            lonlat = lsst.sphgeom.LonLat.fromRadians(coord.getRa().asRadians(),
                                                     coord.getDec().asRadians())
            vertices.append(lsst.sphgeom.UnitVector3d(lonlat))
        return lsst.sphgeom.ConvexPolygon(vertices)

    def __init__(self, skyMap, tracts_file='tracts.pkl'):
        self.skyMap = skyMap
        self.tracts = {}
        self.patches = {}
        tinfo_path = tracts_file
        if os.path.isfile(tinfo_path):
            with open(tinfo_path, 'rb') as handle:
                self.tracts = pickle.load(handle)
            print('retrieving tract info from %s' % tinfo_path)
        else:
            for n, tractInfo in enumerate(self.skyMap):
                if n % 100 == 0 and n > 0:
                    print("Prepping tract %d of %d" % (n, len(self.skyMap)))
                self.tracts[tractInfo.getId()] = self.makeBoxWcsRegion(
                    tractInfo.getBBox(),
                    tractInfo.getWcs()
                )
            with open(tinfo_path, 'wb') as handle:
                pickle.dump(self.tracts, handle)

    def _ensurePatches(self, tract):
        if tract not in self.patches:
            patches = {}
            tractInfo = self.skyMap[tract]
            for patchInfo in tractInfo:
                patches[patchInfo.getIndex()] = self.makeBoxWcsRegion(
                    patchInfo.getOuterBBox(),
                    tractInfo.getWcs()
                )
            self.patches[tract] = patches

    def findOverlaps(self, box, wcs, margin=100):
        polygon = self.makeBoxWcsRegion(box=box, wcs=wcs, margin=margin)
        results = []
        for tract, tractPoly in self.tracts.items():
            if polygon.relate(tractPoly) != lsst.sphgeom.DISJOINT:
                self._ensurePatches(tract)
                results.append(
                    (tract,
                     [patch for patch, patchPoly in self.patches[tract].items()
                      if polygon.relate(patchPoly) != lsst.sphgeom.DISJOINT])
                )
        return results


def wcs_from_boresight(ratel, dectel, rotangle, detector):
    """Return an estimate of the WCS for the specified detector."""
    ra = lsst.geom.Angle(ratel, lsst.geom.radians)
    dec = lsst.geom.Angle(dectel, lsst.geom.radians)
    rotskypos = lsst.geom.Angle(rotangle, lsst.geom.radians)
    boresight = lsst.geom.SpherePoint(ra, dec)
    return createInitialSkyWcsFromBoresight(boresight, rotskypos, detector)


class OverlapFinder:
    """Class to compute overlaps of sensor-visits with a sky map."""
    def __init__(self, opsim_db_file, skymap_polygons, seed=42,
                 opsim_version=2, visit_range=None):
        """
        Parameters
        ----------
        opsim_db_file : str
            OpSim database cadence file.
        skymap_polygons : SkyMapPolygons
            Object containing the ConvexPolygons for the patches in the
            sky map.
        seed : int [42]
            Seed for the random number generator.
        """
        self.opsim_version = opsim_version
        with sqlite3.connect(opsim_db_file) as con:
            if opsim_version == 1:
                query = ('select obsHistID, descDitheredRA, descDitheredDec, '
                         'filter from summary')
                if visit_range is not None:
                    query += (f' where {visit_range[0]} <= obsHistID and '
                              f'obsHistID <= {visit_range[1]}')
                self.opsim_db = pd.read_sql(query, con)
            elif opsim_version == 2:
                query = ('select observationId, fieldRA, fieldDec, filter '
                         'from observations')
                if visit_range is not None:
                    query += (f' where {visit_range[0]} <= observationId and '
                              f'observationId <= {visit_range[1]}')
                self.opsim_db = pd.read_sql(query, con)
        self.skymap_polygons = skymap_polygons
        self.rng = np.random.RandomState(seed)

    def get_overlaps(self, visits, margin=10, camera=LSSTCAM):
        """
        Compute the overlaps of sensor-visits from the list of visits
        with the sky map.

        Parameters
        ----------
        visits : list-like
            A list of visits to process.

        Returns
        -------
        pandas.DataFrame with the overlap info.
        """
        dfs = []
        for i, visit in enumerate(visits):
            print(i, len(visits))
            sys.stdout.flush()
            if self.opsim_version == 1:
                visit_info = self.opsim_db.query(f'obsHistID=={visit}').iloc[0]
                ratel = visit_info['descDitheredRA']
                dectel = visit_info['descDitheredDec']
            elif self.opsim_version == 2:
                visit_info \
                    = self.opsim_db.query(f'observationId=={visit}').iloc[0]
                ratel = np.radians(visit_info['fieldRA'])
                dectel = np.radians(visit_info['fieldDec'])
            rotangle = self.rng.uniform(0, 2*np.pi)

            data = defaultdict(list)
            for detector in camera:
                if detector.getType() != DetectorType.SCIENCE:
                    continue
                wcs  = wcs_from_boresight(ratel, dectel, rotangle, detector)
                bbox = detector.getBBox()
                for tract, patches in \
                    self.skymap_polygons.findOverlaps(bbox, wcs, margin=margin):
                    for patch in patches:
                        data['tract'].append(tract)
                        data['patch'].append('{},{}'.format(*patch))
                        data['visit'].append(visit)
                        data['detector'].append(detector.getId())
                        data['band'].append(visit_info['filter'])
            dfs.append(pd.DataFrame(data=data))
        return pd.concat(dfs)


def extract_coadds(df, bands='ugrizy', verbose=False):
    """
    Extract the coadds for each band-tract-patch combination, and
    compute the number of visits per coadd for resource scaling.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe containing the overlaps table information, i.e.,
        overlap of sensor-visits with patches in the skymap.
    bands : list-like ['ugrizy']
        Bands to consider, e.g., the standard ugrizy bands for Rubin.
    verbose : bool [False]
        Verbosity flag.

    Returns
    -------
    pandas.DataFrame with the band, tract, patch, num_visits columns.
    """
    band_colname = 'band' if 'band' in df else 'filter'
    data = defaultdict(list)
    for band in bands:
        band_df = df.query(f'{band_colname} == "{band}"')
        tracts = set(band_df['tract'])
        for i, tract in enumerate(tracts):
            tract_df = band_df.query(f'tract == {tract}')
            if verbose:
                print(i, band, tract, len(tracts))
            patches = set(tract_df['patch'])
            for patch in patches:
                my_df = tract_df.query(f'patch == "{patch}"')
                data['band'].append(band)
                data['tract'].append(tract)
                data['patch'].append(patch)
                data['num_visits'].append(len(set(my_df['visit'])))
    return pd.DataFrame(data=data)

def unique_tuples(df, columns):
    """
    Return the set of unique tuples from a dataframe for the specified
    columns.
    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe with the columns of data to be considered.
    columns : list-like
        A list of column names in the dataframe from which to
        construct the tuples.
    """
    return set(zip(*[df[_] for _ in columns]))
