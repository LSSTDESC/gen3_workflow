# This file is part of obs_lsst.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

config.skyMap.name = "rings"
config.skyMap["rings"].numRings = 120
config.skyMap["rings"].projection = "TAN"
config.skyMap["rings"].tractOverlap = 1.0/60 # Overlap between tracts (degrees)
config.skyMap["rings"].pixelScale = 0.2
config.skyMap["rings"].patchInnerDimensions = [2000, 2000]
config.name = "DC2-small-patches"
