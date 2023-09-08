import numpy as np
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib import patches
import lsst.geom
import lsst.sphgeom


__all__ = ['SurveyRegion', 'make_patch', 'sky_center',
           'plot_tract', 'plot_tract_patches']


class SurveyRegion:
    def __init__(self, ra0, dec0, lon_size, lat_size):
        self.cos_dec = np.cos(np.radians(dec0))
        self.ra_min = ra0 - lon_size/2./self.cos_dec
        self.ra_max = ra0 + lon_size/2./self.cos_dec
        self.dec_min = dec0 - lat_size/2.
        self.dec_max = dec0 + lat_size/2.
        self._make_polygon()

    def _make_polygon(self):
        corners = [(self.ra_min, self.dec_min),
                   (self.ra_max, self.dec_min),
                   (self.ra_max, self.dec_max),
                   (self.ra_min, self.dec_max)]
        vertices = [lsst.sphgeom.UnitVector3d(
            lsst.sphgeom.LonLat.fromDegrees(*corner)) for corner in corners]
        self.polygon = lsst.sphgeom.ConvexPolygon(vertices)

    def intersects(self, polygon):
        return self.polygon.intersects(polygon)

    def draw_boundary(self, color=None):
        ra = (self.ra_min, self.ra_max, self.ra_max, self.ra_min, self.ra_min)
        dec = (self.dec_min, self.dec_min, self.dec_max, self.dec_max,
               self.dec_min)
        plt.plot(ra, dec, color=color)

    def get_patch_overlaps(self, skymap):
        overlaps = []
        tracts = [_ for _ in skymap if self.intersects(_.outer_sky_polygon)]
        for tract in tracts:
            for patch in tract:
                if self.intersects(patch.outer_sky_polygon):
                    overlaps.append((tract.getId(), patch.getIndex()))
        return overlaps


def make_patch(vertexList, wcs=None):
    """
    Return a Path in sky coords from vertex list in pixel coords.

    Parameters
    ----------
    vertexList: list of coordinates
        These are the corners of the region to be plotted either in
        pixel coordinates or sky coordinates.
    wcs: lsst.geom.skyWcs.skyWcs.SkyWcs [None]
        The WCS object used to convert from pixel to sky coordinates.

    Returns
    -------
    matplotlib.path.Path: The encapsulation of the vertex info that
        matplotlib uses to plot a patch.
    """
    if wcs is not None:
        skyPatchList = [wcs.pixelToSky(pos).getPosition(lsst.geom.degrees)
                        for pos in vertexList]
    else:
        skyPatchList = vertexList
    verts = [(coord[0], coord[1]) for coord in skyPatchList]
    verts.append((0, 0))
    codes = [Path.MOVETO,
             Path.LINETO,
             Path.LINETO,
             Path.LINETO,
             Path.CLOSEPOLY,
             ]
    return Path(verts, codes)


def sky_center(tract_or_patch):
    return tract_or_patch.wcs.pixelToSky(tract_or_patch.getOuterBox()
                                         .getCenter())\
                             .getPosition(lsst.geom.degrees)


def plot_tract(skymap, tract_id, color='blue', ax=None, fontsize=10,
               write_label=True):
    """
    Plot a tract from a SkyMap.

    Parameters
    ----------
    skymap: lsst.skyMap.SkyMap
        The SkyMap object containing the tract and patch information.
    tract_id: int
        The tract id of the desired tract to plot.
    color: str ['blue']
        Color to use for rendering the tract and tract label.
    ax : matplotlib.Axes object [None]
        Axes on which to plot the tract. If None, use the current axes.
    fontsize: int [10]
        Size of font to use for tract label.
    write_label: bool [True]
        Write the label of the tract on the plot.
    """
    if ax is None:
        ax = plt.gca()
    tract_info = skymap[tract_id]
    tractBox = lsst.geom.Box2D(tract_info.getBBox())
    wcs = tract_info.getWcs()
    tract_center = wcs.pixelToSky(tractBox.getCenter())\
                      .getPosition(lsst.geom.degrees)
    if write_label:
        ax.text(tract_center[0], tract_center[1], '%d' % tract_id,
                size=fontsize, ha="center", va="center", color='blue')
    path = make_patch(tractBox.getCorners(), wcs)
    patch = patches.PathPatch(path, alpha=0.1, lw=1, color=color)
    ax.add_patch(patch)

    return ax


def plot_tract_patches(skymap, tract=0, title=None, ax=None,
                       patch_colors=None, survey_region=None):
    """
    Plot the patches for a tract from a SkyMap.

    Parameters
    ----------
    skymap: lsst.skyMap.SkyMap
        The SkyMap object containing the tract and patch information.
    tract: int [0]
        The tract id of the desired tract to plot.
    title: str [None]
        Title of the tract plot.  If None, the use `tract <id>`.
    ax: matplotlib.axes._subplots.AxesSubplot [None]
        The axes object to contain the tract plot.  If None, then
        use the current axes from plt.gca.
    patch_colors: dict [None]
        Dictionary of colors keyed by patchId.
    survey_reiogn : SurveyRegion
        SurveyRegion object defining the boundaries of the survey.

    Returns
    -------
    matplotlib.axes._subplots.AxesSubplot: The subplot containing the
    tract plot.
    """
    if title is None:
        title = 'tract {}'.format(tract)
    tract_info = skymap[tract]
    tractBox = lsst.geom.Box2D(tract_info.getBBox())
    tractPosList = tractBox.getCorners()
    wcs = tract_info.getWcs()
    xNum, yNum = tract_info.getNumPatches()

    if ax is None:
        ax = plt.gca()

    tract_center = wcs.pixelToSky(tractBox.getCenter())\
                      .getPosition(lsst.geom.degrees)
    ax.text(tract_center[0], tract_center[1], '%d' % tract, size=16,
            ha="center", va="center", color='blue')
    for x in range(xNum):
        for y in range(yNum):
            patch_info = tract_info.getPatchInfo([x, y])
            if (survey_region is not None
                and not survey_region.intersects(patch_info.outer_sky_polygon)):
                continue
            patchBox = lsst.geom.Box2D(patch_info.getOuterBBox())
            pixelPatchList = patchBox.getCorners()
            path = make_patch(pixelPatchList, wcs)
            try:
                color = patch_colors[(x, y)]
            except (TypeError, KeyError):
                color = 'blue'
            patch = patches.PathPatch(path, alpha=0.1, lw=1, color=color)
            ax.add_patch(patch)
            center = wcs.pixelToSky(patchBox.getCenter())\
                        .getPosition(lsst.geom.degrees)
            ax.text(center[0], center[1], '%d,%d' % (x, y), size=6,
                    ha="center", va="center")

    skyPosList = [wcs.pixelToSky(pos).getPosition(lsst.geom.degrees)
                  for pos in tractPosList]
    ax.set_xlim(max(coord[0] for coord in skyPosList) + 1,
                min(coord[0] for coord in skyPosList) - 1)
    ax.set_ylim(min(coord[1] for coord in skyPosList) - 1,
                max(coord[1] for coord in skyPosList) + 1)
    ax.grid(ls=':', color='gray')
    ax.set_xlabel("RA (deg.)")
    ax.set_ylabel("Dec (deg.)")
    ax.set_title(title)
    return ax
