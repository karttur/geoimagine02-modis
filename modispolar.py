'''
Created on 18 Feb 2021

@author: thomasgumbricht
'''

# Standard library imports

from sys import exit

# Third party imports

# Package application imports

from geoimagine.assets import AccessOnlineData

class ProcessModisEase2N(AccessOnlineData):
    'class for modis specific processing' 
      
    def __init__(self, pp, session):
        '''
        '''
        
        # Initiate the pacakge for Online data access
        AccessOnlineData.__init__(self)
        
        self.session = session
                
        self.pp = pp  
        
        self.verbose = self.pp.process.verbose 
        
        self.session._SetVerbosity(self.verbose)

        print ('        ProcessModisPolar:',self.pp.process.processid) 
               
        #direct to subprocess
        
        if self.pp.process.processid.lower() == 'searchmodisplarproducts':
            
            self._SearchOnlineProducts()
            
        elif self.pp.process.processid.lower() == 'modispolarsearchtodb':
            
            self._SearchToDB()
            
        elif self.pp.process.processid.lower() == 'downloadmodispolar':
               
            self._DownLoadProduct()
            
        elif self.pp.process.processid.lower() == 'extractmodispolarhdf':
               
            self._ExtractHdf()
            
        else:
            exitstr = 'Exiting, processid %(p)s missing in ProcessModisPolar' %{'p':self.pp.process.processid}
            
            exit(exitstr)
            
             