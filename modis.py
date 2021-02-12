'''
Created on 27 apr. 2018
Last update 12 Feb 2021

@author: thomasgumbricht
'''

# Standard library imports

import os

from sys import exit

from shutil import move, copyfile, copyfileobj

import urllib.request

from html.parser import HTMLParser

# Third party imports

# Package application imports

from geoimagine.params import Composition, LayerCommon, RegionLayer, VectorLayer, RasterLayer

#from ancillary import ancillary_import

import geoimagine.support.karttur_dt as mj_dt 

from geoimagine.gis import GetVectorProjection, GetRasterMetaData, MjProj, Geometry, ESRIOpenGetLayer

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
            exitstr = 'All SMAP compositions must contain a folder'
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
            exitstr = 'EXITING smap FPN contains space %s' %(self.FPN)
            exit(exitstr)
            
    def _SetQuery(self):
        self.query = {'tileid':self.tileid, 'tilefilename':self.FN,'source':self.comp.source,'product':self.comp.product,
                 'version':self.comp.version,'acqdate':self.datum.acqdate, 'doy':self.datum.doy, 'folder':self.comp.folder, 'htile':self.htile, 'vtile':self.vtile}

class ProcessModis:
    'class for modis specific processing' 
      
    def __init__(self, pp, session):
        '''
        '''
        
        self.session = session
                
        self.pp = pp  
        
        self.verbose = self.pp.process.verbose 
        

        print ('        ProcessModis',self.pp.process.processid) 
               
        #direct to subprocess
        if self.pp.process.processid.lower() == 'searchdatapool':
            self._SearchDataPool()
            
        elif self.pp.process.processid.lower() == 'modissearchtodb':
            self._ModisSearchToDB()
            
        elif self.pp.process.processid.lower() == 'linkdefaultregionstomodis':
            self._LinkDefaultRegionsToMODIS()
            
        elif self.pp.process.processid.lower() == 'linkuserregiontomodis':
            self._LinkUserRegionToMODIS()
            
        elif self.pp.process.processid.lower() == 'linkinternaltomodis':
            print (self.process.params.regionLayer, self.process.params.regiontype, self.process.params.tractid)
            self._LinkInternalToMODIS()
             
        elif self.pp.process.processid.lower() == 'downloadmodissingletile':
            self._downloadModisSingleTile() 
            
        elif self.pp.process.processid.lower() == 'downloadmodisregiontiles':
            self._downloadModisRegionTiles() 
            
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
            
            exitstr = 'Exiting, processid %(p)s missing in ProcessModis' %{'p':self.pp.process.processid.processid}
            
            exit(exitstr)
       
    def _DrillIntoDataPool(self): 
        '''
        '''
        self.serverurl = self.process.params.serverurl
        self.version = self.process.params.version
        self.product = self.process.params.product
        if not len(self.version) == 3:
            exit('The modis version must be 3 digits, e.g. "005" or "006"')
        if not self.version.isdigit():
            exit('The modis version must be 3 digits, e.g. "005" or "006"')
        if self.product[0:3] == 'MCD':
            sensorurl = 'MOTA'
        elif self.product[0:3] == 'MOD':
            sensorurl = 'MOLT'
        elif self.product[0:3] == 'MYD':
            sensorurl = 'MOLA'
        else:
            exit('MODIS product is not recognised in SearchDataPool')
        #put the remote search path for the requested dates together
        prodPath ='%s.%s' %(self.product,self.version)
        localPath = os.path.join('/volumes',self.process.dstpath.volume,'DataPoolModis',prodPath)
        if not os.path.exists(localPath):
            os.makedirs(localPath)
        return (sensorurl,prodPath,localPath) 
       
    def _SearchDataPool(self):
        '''IMPORTANT the user credentials must be in a hidden file in users home directory called ".netrc"
        '''
        today = mj_dt.Today()
        sensorurl,prodPath,localPath = self._DrillIntoDataPool()
        cmd ='cd %s;' %(localPath)
        os.system(cmd)
        doneFPN = os.path.join(localPath,'done')

        if not os.path.exists(doneFPN):
            os.makedirs(doneFPN)
        #dates involved in the search
        for datum in self.process.srcperiod.datumD:
            #Skip if datum is later than today
            if self.process.srcperiod.datumD[datum]['acqdate'] > today:
                continue
            #search the datapool
            dateStr = mj_dt.DateToStrPointDate(self.process.srcperiod.datumD[datum]['acqdate'])
            url = os.path.join(self.serverurl,sensorurl,prodPath,dateStr)
            localFPN = os.path.join(localPath,dateStr)

            if os.path.exists(localFPN) and not self.process.overwrite:
                continue
            #Check if the file is in the "done" subfolder
            
            if os.path.exists(os.path.join(doneFPN,dateStr)) and not self.process.overwrite:
                continue
            cmd ='cd %s;' %(localPath)
            cmd ='%(cmd)s /usr/local/bin/wget -L --load-cookies --spider --no-parent ~/.datapoolcookies --save-cookies ~/.datapoolcookies %(url)s' %{'cmd':cmd, 'url':url}
            #print (cmd)
            os.system(cmd)
            
    def _ModisSearchToDB(self):
        '''Load dotapool holdings to local db
            Does not utilize the layer class but take parameters directly from xml
        '''
        today = mj_dt.Today()

        prodPath ='%s.%s' %(self.process.params.product, self.process.params.version)

        localPath = os.path.join('/volumes',self.process.srcpath.volume,'DataPoolModis',prodPath)

        for datum in self.process.srcperiod.datumD:
            #Skip if datum is later than today
            if self.process.srcperiod.datumD[datum]['acqdate'] > today:
                continue
            dateStr = mj_dt.DateToStrPointDate(self.process.srcperiod.datumD[datum]['acqdate'])

            localFPN = os.path.join(localPath,dateStr)
            
            tarFPN = os.path.join(localPath,'done',dateStr)
            if not os.path.exists(os.path.split(tarFPN)[0]):
                os.makedirs(os.path.split(tarFPN)[0])
            if os.path.exists(localFPN):    
                self._ReadMODIShtml(self.session,localFPN,tarFPN,self.process.srcperiod.datumD[datum]['acqdate'])
            elif os.path.exists(tarFPN):    
                pass                 
            else:
                print ('MODIS bulk file missing', localFPN)
                
    def _ReadMODIShtml(self,session,FPN,tarFPN,acqdate):
        tmpFPN,headL = self._ParseModisWgetHTML(FPN)
        session._LoadBulkTiles(self.process.params,acqdate,tmpFPN,headL)
        #move the done file to a subdir called done
        move(FPN,tarFPN)
        
    def _ParseModisWgetHTML(self, FPN):
        headL = ['tileid','tilefilename','source','product','version','acqdate','h','v','hvtile']
        tmpFP = os.path.split(FPN)[0]
        tmpFP = os.path.split(tmpFP)[0]
        tmpFP = os.path.join(tmpFP,'tmpcsv')
        if not os.path.exists(tmpFP):
            os.makedirs(tmpFP)
        tmpFPN = os.path.join(tmpFP,'tilelist.csv')
        FPN = 'file://%(fpn)s' %{'fpn':FPN}
        req = urllib.request.Request(FPN)
        with urllib.request.urlopen(req) as response:
            html = response.read()
        parser = MjHTMLParser()
        parser.SetLists(headL)
        parser.feed(str(html)) 
        WriteCSV(parser.hdfL,tmpFPN)
        return tmpFPN, headL
    
    def _downloadModisRegionTiles(self):
        '''Download tile positions for region
        '''
        self.tempFP = os.path.join('/Volumes',self.process.dstpath.volume, 'modis', 'temp')
        if not os.path.exists(self.tempFP):
            os.makedirs(self.tempFP)   
        if self.process.params.asscript:
            shFN = '%(prod)s-%(region)s.sh' %{'prod':self.process.params.product,'region':self.process.proc.userProj.tractid}
            shFP = os.path.join(self.tempFP, 'script')
            if not os.path.exists(shFP):
                os.makedirs(shFP)
            shFPN = os.path.join(shFP,shFN)
            self.regionscriptF = open(shFPN,'w')
            cmd = 'mkdir -p %(fp)s;\n' %{'fp':shFP}
            self.regionscriptF.write(cmd)
            
        for locus in self.process.srcLayerD:
            self.process.params.htile = self.process.srclocations.locusD[locus]['htile']
            self.process.params.vtile = self.process.srclocations.locusD[locus]['vtile']
            #with the tile set, just call _downloadModisSingleTile
            self._downloadModisSingleTile()
        if self.process.params.asscript:
            self.regionscriptF.close()
            print ('Regionsscript', shFPN)

    def _downloadModisSingleTile(self):
        '''Download a single tile position
        '''
        print ('    Downloading MODIS tile',self.process.params.htile,self.process.params.vtile)
        #create a temp folder to which the download will be directed, only when the download is complete will the data be moved in place
        self.tempFP = os.path.join('/Volumes',self.process.dstpath.volume, 'modis', 'temp')
        if not os.path.exists(self.tempFP):
            os.makedirs(self.tempFP)   
        if self.process.params.asscript:
            shFN = '%(prod)s-%(h)d-%(v)d.sh' %{'prod':self.process.params.product,'h':self.process.params.htile,'v':self.process.params.vtile}
            shFP = os.path.join(self.tempFP, 'script')
            if not os.path.exists(shFP):
                os.makedirs(shFP)
            shFPN = os.path.join(shFP,shFN)
            self.scriptF = open(shFPN,'w')
            
        #Search the data to download
        statusD = {'downloaded': self.process.params.downloaded}
        tiles = self.session._SelectMODISdatapooltilesOntile(self.process.params, self.process.srcperiod, statusD)
 
        dlL = []
        for tile in tiles:

            tileid, hdfFN, source, product, version, acqdate, h, v, hvtile = tile[0:9]
            modisTile = self._ConstructModisTile(tile,self.process.dstpath)
            #Manual test for exists to allow script solution
            if os.path.exists(modisTile.FPN):
                print ('    Already downloaded',tileid) 
                self.session._InsertMODIStile(modisTile.query)
                statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                self.session._UpdateModisTileStatus(statusD)
            else:
                if self.process.params.asscript:
                    cmd = 'mkdir -p %(FP)s;\n' %{'FP':modisTile.FP}
                    self.scriptF.write(cmd)
                    if self.process.proc.processid.lower() == 'downloadmodisregiontiles':
                        self.regionscriptF.write(cmd)
                datedir = mj_dt.DateToStrPointDate(acqdate)
                localTempFPN = os.path.join(self.tempFP,modisTile.FN)
                dlL.append({'query':modisTile.query,'productversion':source,'datedir':datedir,'fn':hdfFN,'dstFPN':modisTile.FPN,'tempFPN':localTempFPN,'tileid':tileid})
        self.AccessMODIS(dlL) 
        if self.process.params.asscript:
            self.scriptF.close()
            print ('run script',shFPN)
            
    def _CheckModisRegion(self):
        '''Check tiles for region
        '''          
        for locus in self.process.srcLayerD:
            self.process.params.htile = self.process.srclocations.locusD[locus]['htile']
            self.process.params.vtile = self.process.srclocations.locusD[locus]['vtile']
            #with the tile set, just call _downloadModisSingleTile
            self._CheckModisSingleTile()

    def _CheckModisSingleTile(self):
        '''Check modis tile and layer status for single tile position
        ''' 
        
        checkdownloaded = self.process.params.checkdownloaded
        checkexploded = self.process.params.checkexploded
        if not checkdownloaded and not checkexploded:
            exit('The process CheckModis must have either, or both, checkdownloaded or checkexploded set to True')
        
        #Set the expected layers and parameters for filling the db
        queryD = {}
        queryD['product'] = {'val':self.process.params.product, 'op':'=' }
        queryD['retrieve'] = {'val':'Y', 'op':'=' }
        self.paramL = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'celltype', 'dataunit', 'scalefac', 'offsetadd', 'cellnull', 'measure', 'retrieve', 'hdffolder', 'hdfgrid']
        self.compL = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'celltype', 'dataunit', 'scalefac', 'offsetadd', 'cellnull', 'measure']
        self.extractL = self.session._SelectMODISTemplate( queryD, self.paramL )

        
        
        #First loop over the src folder structure to find all tiles at this position
        #Construct a dummy tile, to get the FP
        tileid = 'tileid' 
        hdfFN = '*.hdf'
        product = self.process.params.product
        version = self.process.params.version
        source = '%(p)sv%(v)s' %{'p':product,'v':version}
        h = self.process.params.htile
        v = self.process.params.vtile
        acqdate = mj_dt.Today()
        hv = (h,v)
        
        hvD = convTile(hv)
        tile = (tileid, hdfFN, source, product, version, acqdate, h, v, hvD['prstr'])
        print ('    Checking MODIS tile',hvD['prstr'])

        modisTile = self._ConstructModisTile(tile,self.process.srcpath)
        datepath = os.path.split(modisTile.FPN)[0]
        locuspath = os.path.split(datepath)[0]

        #Create a list of the dates included in the process
        includeL = [mj_dt.DateToStrDate(self.process.srcperiod.datumD[date]['acqdate']) for date in self.process.srcperiod.datumL]
        
        self._CheckDBconsistency(locuspath,includeL)
        #Loop the locuspath
        if checkdownloaded:
            if checkexploded:
                print ('        Checking download and exploded')
            else:
                print ('        Checking download')
            tileD = {}
            
            tileD = self._WalkLocuspath(locuspath,includeL,tileD) 
            print ('tileD',tileD)
            for tile in tileD:
                self.session._InsertMODIStile(tileD[tile]['modisTile'].query)
                tileD[tileid] = {'modisTile':modisTile} 
                statusD = {'tileid': tile,'column':'downloaded', 'status': 'N'}
                self.session._UpdateModisTileStatus(statusD)
                if checkexploded:
                    NOTFIXED
                    self._SearchExtractLayers(acqdate,h,v,hvtile)
                    if self.nrExploded == len(self.extractL):
                        if not downloaded:
                            statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                            self.session._UpdateModisTileStatus(statusD)
                        statusD = {'tileid': tileid,'column':'organized', 'status': 'Y'}
                        self.session._UpdateModisTileStatus(statusD)
                        statusD = {'tileid': tileid,'column':'exploded', 'status': 'Y'}
                        self.session._UpdateModisTileStatus(statusD)
            '''
            BALLE
            
            for root, dirs, files in os.walk(locuspath, topdown=True):
                #only retain folders that represent the included dates
                dirs[:] = [d for d in dirs if d in includeL]
                for filename in files:
                    if filename.endswith(self.process.srcpath.hdrfiletype):
                        queryD = {'tilefilename':filename}
                        paramL = ['tileid', 'tilefilename', 'source', 'product', 'version', 'acqdate', 'h', 'v', 'hvtile']
                        tile = self.session._SelectSingleMODISdatapoolTile(queryD,paramL)
                        tileid, hdfFN, source, product, version, acqdate, h, v, hvtile = tile
                        #Construct the tile
                        modisTile = self._ConstructModisTile(tile, self.process.srcpath)
                        downloaded = False
                        if os.path.exists(modisTile.FPN):
                            self.session._InsertMODIStile(modisTile.query)
                            statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                            self.session._UpdateModisTileStatus(statusD)
                            downloaded = True
            '''
            '''
                           
                        if checkexploded:
 
                            self._SearchExtractLayers(acqdate,h,v,hvtile)
                            if self.nrExploded == len(self.extractL):
                                if not downloaded:
                                    statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                                    self.session._UpdateModisTileStatus(statusD)
                                statusD = {'tileid': tileid,'column':'organized', 'status': 'Y'}
                                self.session._UpdateModisTileStatus(statusD)
                                statusD = {'tileid': tileid,'column':'exploded', 'status': 'Y'}
                                self.session._UpdateModisTileStatus(statusD)
            '''
        else:
            #Only checking for exploded
            print ('        Checking expload')
            hvtile = hvD['prstr']
            for date in self.process.srcperiod.datumL:
                acqdate = self.process.srcperiod.datumD[date]['acqdate']
                self._SearchExtractLayers(acqdate,h,v,hvtile)
                if self.nrExploded == len(self.extractL):
                    query = {'h':h, 'v':v, 'acqdate':acqdate,'product':product,'version':version}
                    paramL = ['tileid']
                    tileid = self.session._SelectTileIdOnhvd(query,paramL)
                    if tileid == None:
                        ERRORINGE
                    tileid = tileid[0]

                    statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                    self.session._UpdateModisTileStatus(statusD)
                    statusD = {'tileid': tileid,'column':'organized', 'status': 'Y'}
                    self.session._UpdateModisTileStatus(statusD)
                    statusD = {'tileid': tileid,'column':'exploded', 'status': 'Y'}
                    self.session._UpdateModisTileStatus(statusD)
                    
    def _WalkLocuspath(self,locuspath,includeL,tileD):

        for root, dirs, files in os.walk(locuspath, topdown=True):
            #only retain folders that represent the included dates
            dirs[:] = [d for d in dirs if d in includeL]
            for filename in files:
                if filename.endswith(self.process.srcpath.hdrfiletype):
                    queryD = {'tilefilename':filename}
                    paramL = ['tileid', 'tilefilename', 'source', 'product', 'version', 'acqdate', 'h', 'v', 'hvtile']
                    tile = self.session._SelectSingleMODISdatapoolTile(queryD,paramL)
                    tileid, hdfFN, source, product, version, acqdate, h, v, hvtile = tile
                     
                    #Construct the tile
                    modisTile = self._ConstructModisTile(tile, self.process.srcpath)
                    tileD[tileid] = {'modisTile':modisTile} 
                    #downloaded = False
                    
                    if os.path.exists(modisTile.FPN):
                        tileD[tileid]['downloaded'] = 'Y'
                        #self.session._InsertMODIStile(modisTile.query)
                        #statusD = {'tileid': tileid,'column':'downloaded', 'status': 'Y'}
                        #self.session._UpdateModisTileStatus(statusD)
                        #downloaded = True
                    else:
                        tileD[tileid]['downloaded'] = 'N'
            return tileD

    def _CheckDBconsistency(self,locuspath,includeL):
        '''
        '''
        product = self.process.params.product
        version = self.process.params.version
        source = '%(p)sv%(v)s' %{'p':product,'v':version}
        h = self.process.params.htile
        v = self.process.params.vtile
        
        #Select  exploded but not dowbloaded = should not exist
        startdate =   self.process.srcperiod.datumD[self.process.srcperiod.datumL[0]]['acqdate']  
        enddate =   self.process.srcperiod.datumD[self.process.srcperiod.datumL[-1]]['acqdate']     
        queryD = {'htile':h,'vtile':v,'source':source,'product':product,'version':version}
        queryD['acqdate'] = {'op': '>=', 'val':startdate}
        queryD['acqdate#'] = {'op': '<=', 'val':enddate}
        queryD['downloaded'] = 'N'
        queryD['exploded'] = 'Y'

        tiles = self.session._SelectMODIStiles(queryD)
        if len(tiles) > 0:
            SNULLE
            
        #Check downloaed but not exploded
        queryD['downloaded'] = 'Y'
        queryD['exploded'] = 'N'
        tiles = self.session._SelectMODIStiles(queryD)
        if len(tiles) > 0:
            #checkL = [tile[0] for tile in tiles]
            tileD = {}
            for tile in tiles:
                tileD[tile[0]] = {'downloaded': 'N'}
            # Find out if the tile exists
            tileD = self._WalkLocuspath(locuspath,includeL,tileD)
            #Update the db with non-existing tiles
            for tile in tileD:
                if tileD[tile]['downloaded'] == 'N':
                    statusD = {'tileid': tile,'column':'downloaded', 'status': 'N'}
                    self.session._UpdateModisTileStatus(statusD)
                        
    def _ConstructModisTile(self, tile, sdpath):
        '''
        '''
        tileid, hdfFN, source, product, version, acqdate, h, v, hvtile = tile[0:9]
        #construct the composition
        compD = {'source':source, 'product':product, 'version':version, 'folder':'original', 'system':'modis', 'division':'tiles'}
        #Invoke the composition
        comp = ModisComposition(compD)
        #Set the datum
        datumD = {'acqdatestr': mj_dt.DateToStrDate(acqdate), 'acqdate':acqdate}
        #Set the filename
        FN = hdfFN
        #Set the locus         
        loc = hvtile
        #Set the locuspath
        locusPath = os.path.join(hvtile[0:3],hvtile[3:6])
        #Construct the locus dictionary
        locusD = {'locus':loc, 'path':locusPath, 'h':h, 'v':v}
        #Invoke and return a SentinelTile             
        return ModisTile(tileid, comp, locusD, datumD, sdpath, FN)        
    
    def AccessMODIS(self,dlL):   
        '''
        '''
        sensorurl,prodPath,localPath = self._DrillIntoDataPool()
        for tile in dlL:
            url = os.path.join(self.serverurl,sensorurl,prodPath,tile['datedir'],tile['fn'])
            home = os.path.expanduser("~")
            cookieFPN = os.path.join(home,'.modisdp_cookies')
            cmd = "curl -n -L -c %(c)s -b %(c)s  %(r)s --output %(l)s;" %{'u':self.process.params.remoteuser, 'c':cookieFPN, 'r':url, 'l':tile['tempFPN']}
            cmd = "%(cmd)s mv %(output)s %(dstFPN)s;" %{'cmd':cmd,'output':tile['tempFPN'], 'dstFPN':tile['dstFPN']}
            if self.process.params.asscript:
                cmdL = cmd.split(';')
                for c in cmdL:
                    if len(c) > 1:
                        writeln = '%(c)s;\n' %{'c':c}
                        self.scriptF.write(writeln)
                        if self.process.proc.processid.lower() == 'downloadmodisregiontiles':
                            self.regionscriptF.write(writeln)
            else:
                os.system(cmd)
                self.session._InsertMODIStile(tile['query'])
                statusD = {'tileid': tile['tileid'],'column':'downloaded', 'status': 'Y'}
                self.session._UpdateModisTileStatus(statusD)
                  
    def _LinkDefaultRegionsToMODIS(self):
        '''
        '''
        
   
        for locus in self.pp.srcLayerD:
            
            if len(self.pp.srcLayerD[locus]) == 0:
                
                exitstr = 'EXITING, no dates defined in Sentinel._ExtractSentinelTileCoords'
                
                exit(exitstr)
                
            for datum in self.pp.srcLayerD[locus]:
                
                if len(self.pp.srcLayerD[locus][datum]) == 0:
                    
                    exitstr = 'EXITING, no compositions defined in modis._LinkDefaultRegionsToMODIS'
                    
                    exit(exitstr)
                    
                for comp in self.pp.srcLayerD[locus][datum]:
                    
                    self.srcLayer = self.pp.srcLayerD[locus][datum][comp]
          
 
        self._GetModisTilesDict()

        self._GetSystemDefRegions()
   
    def _LinkUserRegionToMODIS(self):
        '''
        '''
        #Get the modis tiles and create a dict
        self._GetModisTilesDict()
        self.regiontype = 'tract' #should be possible to be site as well
        for locus in self.process.srcLayerD:

            if len(self.process.srcLayerD[locus]) == 0:
                exitstr = 'EXITING, no dates defined in Sentinel._ExtractSentinelTileCoords'
                exit(exitstr)
            for datum in self.process.srcLayerD[locus]:
                if len(self.process.srcLayerD[locus][datum]) == 0:
                    exitstr = 'EXITING, no compositions defined in Sentinel._ExtractSentinelTileCoords'
                    exit(exitstr)
                for comp in self.process.srcLayerD[locus][datum]:
                    if self.process.srcLayerD[locus][datum][comp].comp.id == 'region':
                        regionLayer = self.process.srcLayerD[locus][datum][comp]
                    elif self.process.srcLayerD[locus][datum][comp].comp.id == 'modtiles':
                        self.modtiles = self.process.srcLayerD[locus][datum][comp]
                    else:
                        exitStr = "Error: the process LinkUserRegionToMODIS expects the srcCompds with the id´s 'region' and 'modtiles'"
                        exit(exitStr)
                        
                self._IdentifyOverlap(regionLayer,self.process.proc.userProj.tractid)
                
    def _LinkInternalToMODIS(self):
        '''
        '''
        #Get the modis tiles and create a dict

        self._GetModisTilesDict()
        self.regiontype = self.process.params.regiontype 
        self._IdentifyOverlap(self.process.params.regionLayer, self.process.params.tractid)
                 
    def _GetModisTilesDict(self):
        '''
        '''
        
        from support.modis import ConvertMODISTilesToStr as convTile
        
        withintiles = self.session._SelectModisRegionTiles({'regionid':self.pp.process.parameters.defregmask})
        
        if len(withintiles) == 0:
        
            exitstr = 'Process LinkDefaultRegionsToMODIS can not find any tile region to use for restriction.\n Parameter withintiles: %s' %(self.pp.process.defregmask)
            
            exit(exitstr)
        
        self.rTiles = [convTile(item)['prstr'] for item in withintiles]

        recs = self.session._SelectModisTileCoords()
        
        self.modisTileD ={}
        
        for rec in recs:
        
            hvtile,h,v,minxsin,minysin,maxxsin,maxysin,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon = rec
            
            if hvtile in self.rTiles:

                llptL = ((ullon,ullat),(urlon,urlat),(lrlon,lrlat),(lllon,lllat))
            
                modtilegeom = Geometry()
                
                modtilegeom.PointsToPolygonGeom(llptL)
                
                west, south, east, north = modtilegeom.shapelyGeom.bounds
                
                self.modisTileD[hvtile] = {'hvtile':hvtile,'h':h,'v':v,'geom':modtilegeom,
                                      'west':west,'south':south,'east':east,'north':north}
       
    def _GetSystemDefRegions(self):
        '''
        '''
        #I need the layer for the region that is not in the regionsmodis table
        recs = self.session._SelectAllDefRegions("M.regionid IS NULL")
        
        print (len(recs))
        
        n = 0
        
        for rec in recs:   
             
            n += 1
                  
            print ('    ', n , rec[1])
            
            comp =  self.pp.srcCompD['defreg']
             
            compD = dict( list( comp.__dict__.items() ) )
            
            regionid = rec[1]
                        
            comp = Composition(compD, self.pp.process.parameters, self.pp.procsys.srcsystem, self.pp.procsys.srcdivision, self.pp.srcPath)
                        
            queryD = {'compid':comp.compid, 'regionid': regionid}
            
            paramL = ['source', 'product', 'suffix', 'acqdate', 'acqdatestr', 'doy', 'createdate', 'regionid']

            layerrec = self.session._SelectLayer(comp.system, queryD, paramL)
            
            if layerrec == None:
                
                continue 
            
            comp.source, comp.product, comp.suffix, acqdate, acqdatestr, doy, createdate, regionid = layerrec

            if acqdate == None:
            
                acqdate = False
            
            datumD = {'acqdatestr': acqdatestr, 'acqdate':acqdate}
            
            #Set the locus         
            loc = regionid
            
            #Set the locuspath
            locusPath = regionid
            
            #Construct the locus dictionary
            locusD = {'locus':loc, 'locusPath':locusPath, 'path':locusPath}
            #Create the layer
            
            regionLayer = VectorLayer(comp, locusD, datumD)
            
            self.regiontype = 'default'
            
            print ('        ',regionLayer.FPN)
            
            if os.path.exists(regionLayer.FPN):
                
                print ('        Processing')
            
                self._IdentifyOverlap(regionLayer,regionid)
                

    def _IdentifyOverlap(self,layer,regionid):
        #Get the layer and the geom

        srcDS,srcLayer = ESRIOpenGetLayer(layer.FPN)
        
        for feature in srcLayer.layer:     
                 
            geom = Geometry()
            
            #add the feature and extract the geom
            geom.GeomFromFeature(feature)
            
            if srcLayer.geomtype.lower() != 'polygon':
            
                ERRORIGEN
           
            west, south, east, north = geom.shapelyGeom.bounds
            
            #Get the tiles inside this region
            
            tiles = self.session._SearchTilesFromWSEN(west, south, east, north)
            
            for tile in tiles:
                
                hvtile,htile,vtile,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy = tile
                
                if hvtile in self.rTiles:
                
                    llptL = ((ullon,ullat),(urlon,urlat),(lrlon,lrlat),(lllon,lllat))
                    
                    tilegeom = Geometry()
                    
                    tilegeom.PointsToPolygonGeom(llptL)
                    
                    #Get the overlap
                    
                    overlapGeom = tilegeom.ShapelyIntersection(self.modisTileD[hvtile]['geom'])  
                    
                    productoverlap = overlapGeom.area/tilegeom.shapelyGeom.area
                    
                    if self.regiontype == 'default': 
                    
                        query = {'system':'system', 'table':'regions', 'regionid':regionid,'regiontype':self.regiontype, 'overwrite':False, 'delete':False, 'hvtile':hvtile,'h':htile, 'v':vtile}
                    
                    elif self.regiontype == 'tract': 
                    
                        query = {'system':'regions', 'table':'tracts', 'regionid':regionid,'regiontype':self.regiontype, 'overwrite':False, 'delete':False, 'hvtile':hvtile,'h':htile, 'v':vtile}
                    
                    else:
                    
                        print (self.regiontype)
                        
                        NOREGION
                    
                    if productoverlap >= 0:
                        
                        self.session._InsertModisRegionTile(query)
                 
    def _ExplodeMODISRegion(self):
        '''Explode MODIS regional tiles
        '''          
        self.tempFP = os.path.join('/Volumes',self.process.dstpath.volume, 'modis', 'temp')
        if not os.path.exists(self.tempFP):
            os.makedirs(self.tempFP)   
        if self.process.params.asscript:
            shFN = 'explode-%(prod)s-%(region)s.sh' %{'prod':self.process.params.product,'region':self.process.proc.userProj.tractid}
            shFP = os.path.join(self.tempFP, 'script')
            if not os.path.exists(shFP):
                os.makedirs(shFP)
            shFPN = os.path.join(shFP,shFN)
            self.regionscriptF = open(shFPN,'w')
            cmd = 'mkdir -p %(fp)s;\n' %{'fp':shFP}
            self.regionscriptF.write(cmd)
            
        for locus in self.process.srcLayerD:
            self.process.params.htile = self.process.srclocations.locusD[locus]['htile']
            self.process.params.vtile = self.process.srclocations.locusD[locus]['vtile']
            #with the tile set, just call _downloadModisSingleTile
            self._ExplodeMODISSingleTile()
        if self.process.params.asscript:
            self.regionscriptF.close()
            print ('Regionsscript', shFPN)
                       
    def _ExplodeMODISSingleTile(self):   
        #create a temp folder to which the download will be directed, only when the download is complete will the data be moved in place
        self.tempFP = os.path.join('/Volumes',self.process.dstpath.volume, 'MODIS', 'temp')
        if not os.path.exists(self.tempFP):
            os.makedirs(self.tempFP)
              
        if self.process.params.asscript:
            shFN = 'explode-%(prod)s-%(h)d-%(v)d.sh' %{'prod':self.process.params.product,'h':self.process.params.htile,'v':self.process.params.vtile}
            shFP = os.path.join(self.tempFP, 'script')
            if not os.path.exists(shFP):
                os.makedirs(shFP)
            shFPN = os.path.join(shFP,shFN)
            self.scriptF = open(shFPN,'w')
            
        #Set the parameters and extract layers
        #Get the template - should replicate Senitnel and Landsat tempolate retrieval           
        queryD = {}
        queryD['product'] = {'val':self.process.params.product, 'op':'=' }
        queryD['retrieve'] = {'val':'Y', 'op':'=' }
        self.paramL = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'celltype', 'dataunit', 'scalefac', 'offsetadd', 'cellnull', 'measure', 'retrieve', 'hdffolder', 'hdfgrid']
        self.compL = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'celltype', 'dataunit', 'scalefac', 'offsetadd', 'cellnull', 'measure']
        self.extractL = self.session._SelectMODISTemplate( queryD, self.paramL )
        #Search the data to download
        statusD = {}
        statusD['downloaded'] = 'Y'
        statusD['exploded'] = self.process.params.exploded
        
        paramL =['D.tileid', 'D.tilefilename', 'D.source', 'D.product', 'D.version', 'D.acqdate', 'D.h', 'D.v', 'D.hvtile']
        
        tiles = self.session._SelectMODISdatapooltilesOntile(self.process.params, self.process.srcperiod, statusD, paramL)

        if len(tiles) == 0:
            print ('    No MODIS scenes found')
        for tile in tiles:
            tileid, hdfFN, source, product, version, acqdate, h, v, hvtile = tile
            modisTile = self._ConstructModisTile(tile,self.process.srcpath)
            #Manual test for exists to allow script solution
            if not os.path.exists(modisTile.FPN):
                #Search for the layers to extract anyway
                self._SearchExtractLayers(acqdate,h,v,hvtile)
                if self.nrExploded == len(self.extractL):
                    statusD = {'tileid': tileid,'column':'organized', 'status': 'Y'}
                    self.session._UpdateModisTileStatus(statusD)
                    statusD = {'tileid': tileid,'column':'exploded', 'status': 'Y'}
                    self.session._UpdateModisTileStatus(statusD)
                else:
                    errorstr = 'Error, missing modis tile in _ExplodeMODISSingleTile: %(t)s' %{'t':modisTile.FPN}
                    print (errorstr)
                    print ('DELETE FROM DB')
                continue
        
            fsize = os.path.getsize(modisTile.FPN)

            if fsize < 1000:
                print (modisTile.FPN)
                snullebulle
            self._SearchExtractLayers(acqdate, h, v, hvtile)

            #Here is the explosion
            if len(self.explodeD) > 0:
                self.nrExploded += self._ExplodeHDF(modisTile.FPN,self.explodeD)
            if self.nrExploded == len(self.extractL):
                statusD = {'tileid': tileid,'column':'organized', 'status': 'Y'}
                self.session._UpdateModisTileStatus(statusD)
                statusD = {'tileid': tileid,'column':'exploded', 'status': 'Y'}
                self.session._UpdateModisTileStatus(statusD)
        if self.process.params.asscript:
            self.scriptF.close()
            printstr =  'To actually explode the MODIS tiles, you have to execute the shell script file:\n    %(fpn)s' %{'fpn':shFPN}
            print (printstr)
 
    def _SearchExtractLayers(self,acqdate,h,v,hvtile):
        '''
        '''
        self.nrExploded = 0
        self.explodeD = {}
        for extcomp in self.extractL:
            paramD = dict(zip(self.paramL,extcomp))
            compD = dict(zip(self.compL,extcomp))
          
            comp = Composition(compD, self.process.system.dstsystem, self.process.system.dstdivision)
            #Set the datum
            #THIS IS WHERE MODIS IS CONVERTED TO DOY FORMAT
            datedoy = mj_dt.DateToYYYYDOY(acqdate)

            datumD = {'acqdatestr': datedoy, 'acqdate':acqdate}
            #Set the locus
            locusPath = os.path.join(hvtile[0:3],hvtile[3:6])
            #Construct the locus dictionary
            locusD = {'locus':hvtile, 'htile':h, 'vtile':v, 'path':locusPath}
            filepath = lambda: None
            filepath.volume = self.process.dstpath.volume; filepath.hdrfiletype = self.process.dstpath.hdrfiletype
            #Create a standard raster layer
            layer = RasterLayer(comp, locusD, datumD, filepath)
            if not layer._Exists() or self.process.overwrite:
                self.explodeD[paramD['band']] = {'layer':layer,'params':paramD}
            elif layer._Exists():
                self.session._InsertLayer(layer,self.process.overwrite,self.process.delete)
                self.nrExploded += 1
            
    def _ExplodeHDF(self, hdfFPN, explodeD):
        #  
        nrExploded = 0 
        for band in explodeD:
            tarFPN = explodeD[band]['layer'].FPN
            hdffolder = explodeD[band]['params']['hdffolder']
            hdfgrid = explodeD[band]['params']['hdfgrid']
            #copy the file to memory and extract the hdf straight from memory? 
            cmd = '/Library/Frameworks/GDAL.framework/Versions/2.2/Programs/gdal_translate '
            cmd = '%(cmd)s HDF4_EOS:EOS_GRID:"%(hdf)s":%(folder)s:%(band)s %(tar)s' %{'cmd':cmd,'hdf':hdfFPN,'folder':hdffolder,'band':hdfgrid, 'tar':tarFPN}

            if self.process.params.asscript:
                cmd = '%(cmd)s;\n' %{'cmd':cmd}
                self.scriptF.write(cmd)
                if self.process.proc.processid.lower() == 'explodemodisregion':
                    self.regionscriptF.write(cmd)
            else:   
                os.system(cmd)
                #register band
                if os.path.isfile(tarFPN):
                    nrExploded += 1
                    self.session._InsertLayer(explodeD[band]['layer'],self.process.overwrite,self.process.delete)
                    #explodeD[band]['layer'].RegisterLayer(self.process.proj.system)
                    #_InsertLayer(self,layer,overwrite,delete)
        return nrExploded
           
    def _TileRegionToModis(self):
        '''Tile regional data to MODIS tiles
        '''  
        self.srcLocus = self.process.srclocations.locusL[0]
   
        self.tempFP = os.path.join('/Volumes',self.process.dstpath.volume, 'modis', 'temp')
        if not os.path.exists(self.tempFP):
            if self.process.params.asscript:
                cmd = 'mkdir -p %(fp)s;\n' %{'fp':self.tempFP}
                self.regionscriptF.write(cmd)
            else:
                os.makedirs(self.tempFP)   
        if self.process.params.asscript:
            shFN = 'tile-%(region)s.sh' %{'region':self.process.proc.userProj.tractid}
            shFP = os.path.join(self.tempFP, 'script')
            if not os.path.exists(shFP):
                os.makedirs(shFP)
            shFPN = os.path.join(shFP,shFN)
            self.regionscriptF = open(shFPN,'w')
            
        for locus in self.process.dstLayerD:
            self._TileSingleTileToModis(locus)
        if self.process.params.asscript:
            self.regionscriptF.close()
            print ('Run',shFPN)
            print (self.process.proc.xml)
    
    def _TileSingleTileToModis(self,locus):
        '''
        '''
        from geoimagine.gdalutilities import GDALstuff
        
        for datum in self.process.dstLayerD[locus]:
            for comp in self.process.dstLayerD[locus][datum]:   
                dstLayer = self.process.dstLayerD[locus][datum][comp]

                srcLayer = self.process.srcLayerD[self.srcLocus][datum][comp]

                if not self.process.dstLayerD[locus][datum][comp]._Exists() or self.process.overwrite:
                    queryD = {'hvtile':locus}
                    paramL =['minxsin','minysin','maxxsin','maxysin','ullat','ullon','lrlat','lrlon','urlat','urlon','lllat','lllon']
                    dstCoords = self.session._SelectSingleTileCoords(queryD, paramL)
                    coordsD = dict(zip(paramL,dstCoords))

                    if self.process.dstLayerD[locus][datum][comp]._Exists():
                        os.remove(self.process.dstLayerD[locus][datum][comp].FPN)

                    GDALwarp = GDALstuff(self.process.srcLayerD[self.srcLocus][datum][comp].FPN, self.process.dstLayerD[locus][datum][comp].FPN, self.process.params)
                    GDALwarp.SetClipBox(coordsD['minxsin'], coordsD['maxysin'], coordsD['maxxsin'], coordsD['minysin'])
                    GDALwarp.SetTargetProj(self.process.params.epsg)
                    gdalcmd = GDALwarp.WarpRaster(self.process.params.asscript)

                    if self.process.params.asscript:
                        cmd = 'mkdir -p %(fp)s;\n' %{'fp':self.process.dstLayerD[locus][datum][comp].FP}
                        self.regionscriptF.write(cmd)
                        self.regionscriptF.write(gdalcmd)
                        self.regionscriptF.write('\n')
                if self.process.dstLayerD[locus][datum][comp]._Exists():

                    self.session._InsertLayer(self.process.dstLayerD[locus][datum][comp],self.process.overwrite,self.process.delete)
                
    def _ResampleSpatial(self):
        '''
        '''
        from geoimagine.gdalutilities import GDALstuff
        for locus in self.process.dstLayerD:
            for datum in self.process.dstLayerD[locus]:
                for comp in self.process.dstLayerD[locus][datum]:   
                    if not self.process.dstLayerD[locus][datum][comp]._Exists() or self.process.overwrite:    
                        if os.path.exists( self.process.dstLayerD[locus][datum][comp].FPN):
                            os.remove(self.process.dstLayerD[locus][datum][comp].FPN)
                        if os.path.exists( self.process.srcLayerD[locus][datum][comp].FPN):
                            GDALtranslate = GDALstuff(self.process.srcLayerD[locus][datum][comp].FPN, self.process.dstLayerD[locus][datum][comp].FPN, self.process.params)
                            gdalcmd = GDALtranslate.ResampleRaster(self.process.params.asscript)
        
                            if self.process.params.asscript:
                                cmd = 'mkdir -p %(fp)s;\n' %{'fp':self.process.dstLayerD[locus][datum][comp].FP}
                                self.regionscriptF.write(cmd)
                                self.regionscriptF.write(gdalcmd)
                                self.regionscriptF.write('\n')
                    if self.process.dstLayerD[locus][datum][comp]._Exists():
                        self.session._InsertLayer(self.process.dstLayerD[locus][datum][comp],self.process.overwrite,self.process.delete)

    def _MosaicModis(self):
        '''    
        '''
        for locus in self.process.dstLayerD:
            for datum in self.process.dstLayerD[locus]:
                for comp in self.process.dstLayerD[locus][datum]:
                    if self.process.dstLayerD[locus][datum][comp]._Exists() and not self.process.overwrite:
                        #self.session._InsertLayer(self.process.dstLayerD[locus][datum][comp],self.process.overwrite,self.process.delete)
                        #Regions are not registered in the db - test aded 20181202
                        pass
                    else:
                        if self.process.dstLayerD[locus][datum][comp]._Exists():
                            os.remove(self.process.dstLayerD[locus][datum][comp].FPN)
                        # Mosaic layers
                        self._MosaicLayers(datum,comp)
                        #clip layers
                        self._ClipLayer(datum,comp)
                        #remove the virtual copy (the vrt mosaic)
                        os.remove(self.vrtFPN)
         
    def _MosaicLayers(self,datum,comp):
        '''
        '''
        self.dstLocus = list(self.process.dstLayerD.keys())[0]
        from geoimagine.gdalutilities import GDALstuff
        inputFileListFP = os.path.join('/Volumes',self.process.dstpath.volume, 'modis', 'temp')
        if not os.path.exists(inputFileListFP):
            os.makedirs(inputFileListFP)
        inputFileListFPN = os.path.join(inputFileListFP,'mosaic.txt')
        f = open(inputFileListFPN,'w')
         
        for locus in self.process.srcLayerD:
            writeln = '%(fpn)s\n' %{'fpn':self.process.srcLayerD[locus][datum][comp].FPN}
            f.write(writeln)
        f.close()
        #replace the target extension with vrt
        vrtFPN = os.path.splitext(self.process.dstLayerD[self.dstLocus][datum][comp].FPN)[0]
        self.vrtFPN = '%(fpn)s.vrt' %{'fpn':vrtFPN}
        GDALmosaic = GDALstuff(inputFileListFPN, self.vrtFPN, self.process.params)
        GDALmosaic.MosaicRaster()
        
    def GetSrcLayerGeoformats(self,key,datum):
        for location in self.srcData.locationidL:
            self.srcData.GetGeoFormat(key,location,datum)
  
    def _ClipLayer(self,datum,comp):
        from geoimagine.gdalutilities import GDALstuff
        regExt = self.session._SelectRegionLonLatExtent(self.process.proc.userProj.defregion,self.process.proc.userProj.defregtype)
        #set the boundary to cut at to the lonlat epsg for now
        #self.process.params.bounds_epsg = 4326
        GDALclip = GDALstuff(self.vrtFPN, self.process.dstLayerD[self.dstLocus][datum][comp].FPN, self.process.params)        
        GDALclip.SetClipBoxLLminmax(regExt)

        GDALclip.ClipRaster()
        
    def SetDstComp(self):
        self.process.xparamTagD = {}
        #self.process.proj.dstSystem = 'region'
        for compKey in self.srcData.compD:
            self.process.xparamTagD['bandout'] = {}
            self.process.xparamTagD['bandout'][compKey] = {}
            self.process.xparamTagD['bandout'][compKey]['scenes'] = True
            self.process.xparamTagD['bandout'][compKey]['division'] = 'region'
           
class MjHTMLParser(HTMLParser):
    def SetLists(self,headL):
        self.hdfL = []
        self.xmlL = []   
        self.hdfL.append(headL)
        self.xmlL.append(headL)
    
    def SplitModisFileName(self,value):
        FNparts = value.split('.')
        product = FNparts[0]
        #doy = int(FNparts[1][5:8])
        acqYYYYdoy = FNparts[1][1:8]
        acqdate = mj_dt.yyyydoyDate(acqYYYYdoy)
        #htile = int(FNparts[2][1:3])
        #vtile = int(FNparts[2][4:6])
        version = FNparts[3]
        #prodid = FNparts[4]
        #filetype = FNparts[len(FNparts)-1]
        source = '%(prod)sv%(v)s' %{'prod':product,'v':FNparts[3]}
        tileid = '%(prod)s-%(v)s-%(yyyydoy)s-%(hv)s' %{'prod':product,'v':FNparts[3], 'yyyydoy':FNparts[1][1:8],'hv':FNparts[2] }
        hdfL = [tileid, value, source, product, version, acqdate]
        #D = {'tileid':tileid,'version':version,'tilefilename':value,'source':'MODIS','product':product,'acqdate':acqdate,'doy':doy,'folder':'orignal','htile':htile,'vtile':vtile}
        self.hdfL.append(hdfL)
        
    def handle_starttag(self, tag, attrs):
        # Only parse the 'anchor' tag.
        if tag == "a":
            # Check the list of defined attributes.
            for name, value in attrs:
                # If href is defined, print it.
                if name == "href":
                    ext = os.path.splitext(value)[1]
                    if ext.lower() == '.hdf':
                        self.SplitModisFileName(value)

def WriteCSV(csvL,tmpFPN):
    import csv
    with open(tmpFPN, 'w') as csvfile:
        wr = csv.writer(csvfile)
        for x,row in enumerate(csvL):
            if x > 0:
                hvtile = row[0].split('-')[3]
                h = int(hvtile[1:3])
                v = int(hvtile[4:6])
                row.extend([h,v,hvtile])
            wr.writerow(row)
