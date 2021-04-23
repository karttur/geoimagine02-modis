'''
Created on 27 apr. 2018
Last update 12 Feb 2021

@author: thomasgumbricht
'''

# Standard library imports

import os

from sys import exit

# Third party imports

# Package application imports

from geoimagine.params import Composition, LayerCommon, RegionLayer, VectorLayer, RasterLayer

#from ancillary import ancillary_import

import geoimagine.support.karttur_dt as mj_dt 

from geoimagine.gis import GetVectorProjection, GetRasterMetaData, MjProj, Geometry, ESRIOpenGetLayer

from geoimagine.assets import AccessOnlineData

class ModisComposition:
    '''
    class for MODIS tile compositions
    '''
    def __init__(self, compD):  
        for key in compD:
            if '_' in compD[key]:
                exitstr = 'the "%s" parameter can not contain underscore (_): %s ' %(key, compD[key])
                exit(exitstr) 
            setattr(self, key, compD[key])
        if not hasattr(self, 'folder'):
            exitstr = 'All modis compositions must contain a folder'
            exit(exitstr)

class ModisTile(LayerCommon):
    '''Class for sentinel tiles'''
    def __init__(self, tileid,composition, locusD, datumD, filepath, FN): 
        """The constructor expects an instance of the composition class."""
        LayerCommon.__init__(self)
        self.tileid = tileid
        self.comp = composition
        
        self.locus = locusD['locus']
        self.locuspath = locusD['path']
        self.htile = locusD['h']
        self.vtile = locusD['v']
        self.path = filepath
        self.FN = FN

        self.datum = lambda: None
        for key, value in datumD.items():
            setattr(self.datum, key, value)
        if self.datum.acqdate:
            self._SetDOY()
            self._SetAcqdateDOY()
        self._SetPath()
        self._SetQuery()
        
    def _SetPath(self):
        """Sets the complete path to sentinel tiles"""
        
        self.FP = os.path.join('/Volumes',self.path.volume, self.comp.system, self.comp.source, self.comp.division, self.comp.folder, self.locuspath, self.datum.acqdatestr)
        self.FPN = os.path.join(self.FP,self.FN)
        if ' ' in self.FPN:
            exitstr = 'EXITING modis FPN contains space %s' %(self.FPN)
            exit(exitstr)
            
    def _SetQuery(self):
        self.query = {'tileid':self.tileid, 'tilefilename':self.FN,'source':self.comp.source,'product':self.comp.product,
                 'version':self.comp.version,'acqdate':self.datum.acqdate, 'doy':self.datum.doy, 'folder':self.comp.folder, 'htile':self.htile, 'vtile':self.vtile}
            
class ProcessModis(AccessOnlineData):
    'class for modis specific processing' 
      
    def __init__(self, pp, session):
        '''
        '''
        
        # Initiate the package for Online data access
        AccessOnlineData.__init__(self)
        
        self.session = session
                
        self.pp = pp  
        
        self.verbose = self.pp.process.verbose 
        
        self.session._SetVerbosity(self.verbose)
        
        print ('        ProcessModis',self.pp.process.processid) 
               
        #direct to subprocess
        
        if self.pp.process.processid.lower() == 'searchmodisproducts':
            
            # Redirect to assets
            self._SearchOnlineProducts('modisdatapool')
            
        elif self.pp.process.processid.lower() == 'modisnsidcsearchtodb':
            
            self._ModisNSIDCSearchToDB()
            
        elif self.pp.process.processid.lower() == 'searchdatapool':
            
            self._SearchDataPool()
            
        elif self.pp.process.processid.lower() == 'searchusgsproducts':
            
            self._SearchOnlineProducts(self.pp.process.parameters.product)
            
        elif self.pp.process.processid.lower() == 'modissearchtodb':
            
            self._SearchToDB('modisdatapool')
            
        elif self.pp.process.processid.lower() == 'downloadusgs':
            
            self._SearchToDB(self.pp.process.parameters.product)
            
        elif self.pp.process.processid.lower() == 'linkdefaultregionstomodis':
            self._LinkDefaultRegionsToMODIS()
            
        elif self.pp.process.processid.lower() == 'linkuserregiontomodis':
            self._LinkUserRegionToMODIS()
            
        elif self.pp.process.processid.lower() == 'linkinternaltomodis':
            print (self.process.params.regionLayer, self.process.params.regiontype, self.process.params.tractid)
            self._LinkInternalToMODIS()
             
        elif self.pp.process.processid.lower() == 'downloadmodissingletile':
            
            self._DownloadTileProduct('modisdatapool',self.pp.process.parameters.asscript) 
            
        elif self.pp.process.processid.lower() == 'downloadmodisregiontiles':
            
            self._DownloadRegionTileProduct('modisdatapool',self.pp.process.parameters.asscript) 
            
        elif self.pp.process.processid.lower() == 'explodemodisregion':
            self._ExplodeMODISRegion()
            
        elif self.pp.process.processid.lower() == 'explodemodissingletile':
            self._ExplodeMODISSingleTile()
         
        elif self.pp.process.processid.lower() == 'checkmodissingletile':
            self._CheckModisSingleTile()   
            
        elif self.pp.process.processid.lower() == 'checkmodisregion':
            self._CheckModisRegion() 
            
        elif 'resamplespatial' in self.pp.process.processid.lower():
            self._ResampleSpatial() 
            
        elif 'tileregiontomodis' in self.pp.process.processid.lower():
            self._TileRegionToModis() 
            
        elif self.pp.process.processid.lower() == 'mosaicmodis':
            self._MosaicModis() 

        else:
            
            exitstr = 'Exiting, processid %(p)s missing in ProcessModis' %{'p':self.pp.process.processid}
            
            exit(exitstr)
       
       
    