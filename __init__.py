"""
Created 22 Jan 2021
Last updated 12 Feb 2021

modis
==========================================

Package belonging to Karttur´s GeoImagine Framework.

Author
------
Thomas Gumbricht (thomas.gumbricht@karttur.com)

"""
from .version import __version__, VERSION, metadataD

from .modis import ProcessModis
from geoimagine.modis.modispolar import ProcessModisEase2N