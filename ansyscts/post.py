#basic libraries
from dataclasses import dataclass
import os
import pandas as pd
from typing import Tuple
from pathlib import Path
import numpy as np
from functools import cached_property


#other .git libraries, need to make sure these are on PYTHONPATH or PAH
from sciterp.mesh_transfer import LinearPointCloudMeshTransfer 
from linearization_lib.linearization.linearization import APDLIntegrate
from linearization_lib.linearization.scl import SCL
from linearization_lib.linearization.pair_component_nodes import LSANodePairer
from sim_datautil.sim_datautil.dutil import SimulationData, SimulationDatabase
import logging
logger = logging.getLogger("ansyscts")

from ansyscts.miscutil import _get_allocated_cores
import ansyscts.config as config

#globals for post
xcols = [x + '-coordinate' for x in ['x','y','z']]
stress_cols = ['xx','yy','zz','xy','yz','xz']   #checked this against output format for wssnol.mac
temperature_cols = ['temperature']

@dataclass
class APDLDataFiles:

    """
    dataclass for the ANSYS APDL data files
    """
    name: str

    def _wnf(self,f: str) -> str:
        if not os.path.exists(f):
            logger.error(f'{f} does not exist')
        
        return f 
    
    @property
    def location_file(self):
        return self._wnf(self.name + '.node.loc')
    
    @property
    def stress_file(self):
        return self._wnf(self.name + '.node.dat')
    
    @property
    def temperature_file(self):
        return self._wnf(self.name + '.node.cfdtemp')

    @property
    def face1_file(self):
        return self._wnf(self.name + '.face1.node.loc')
    
    @property
    def face2_file(self):
        return self._wnf(self.name + '.face2.node.loc')
    
    def remove_files(self):
        """
        will want to do some basic cleanup
        """
        for file in [self.location_file,self.stress_file,self.temperature_file,self.face1_file,self.face2_file]:
            if os.path.exists(file):
                os.remove(file)
                logger.info(f'{file} deleted')
            else:
                logger.warning(f'{file} does not exist, cannot delete.')

         
def read_apdl_node_file(file: str) -> pd.DataFrame:
    """
    read an ANSYS APDL node file and return a pandas dataframe

    Parameters
    ----------
    file: str
        the path to the ANSYS APDL node file

    Returns
    -------
    pd.DataFrame
        a pandas dataframe with the node locations
    """
    node_loc = pd.read_csv(file,index_col = 0,header = None)
    node_loc.index = node_loc.index.astype(int)
    return node_loc

@dataclass
class LinearizedSection:

    """
    Data class for linearizing field data in a sectino of the model
    Needs the body containing caretsian coordinates and the two faces
    with cartesian coordinates that define the section

    Parameters
    ----------
    body: np.ndarray
        the body containing the cartesian coordinates
    face1: pd.DataFrame 
        the first face containing the cartesian coordinates
    face2: pd.DataFrame
        the second face containing the cartesian coordinates
    """
    body: np.ndarray
    face1: pd.DataFrame
    face2: pd.DataFrame

    def __post_init__(self):
        self.loc1 = None
        self.loc2 = None    
    
    def pair(self) -> Tuple[np.ndarray,np.ndarray]:
        node_pair = LSANodePairer.from_locations(self.face1,self.face2)
        paired = node_pair.pair()
        self.loc1 = self.face1.loc[paired[:,0]]
        self.loc2 = self.face2.loc[paired[:,1]]
        return self.loc1,self.loc2

    @cached_property
    def bounded_body(self):
        xmax = np.max(np.array(
            [self.face1.to_numpy().max(axis = 0),
             self.face2.to_numpy().max(axis = 0)])).max(axis = 0)
        xmin = np.min(np.array(
            [self.face1.to_numpy().min(axis = 0),
             self.face2.to_numpy().min(axis = 0)])).min(axis = 0)
        return (self.body >= xmin).all(axis = 1) & (self.body <= xmax).all(axis = 1)
    
    def linearize(self,data: np.ndarray,
                       npoints = 47) -> APDLIntegrate:
        
        """
        linearize the data in the section
        makes the stress classification lines and interpolates the data to them,
        then intergrates the data over the SCL's in the section

        Parameters
        ----------
        data: np.ndarray
            the data to be linearized
        npoints: int
            the number of points to integrate the data over
        
        Returns
        -------
        APDLIntegrate
            the integrated data object
        """
        if data.shape[0] != self.body.shape[0]:
            raise ValueError('data must have the same number of rows as the body')
        
        if self.loc1 is None or self.loc2 is None:
            self.pair()
        
        scl_apdl = SCL(self.loc1.to_numpy(),self.loc2.to_numpy())
        scl_points = scl_apdl(npoints,flattened = True)

        #intentiionally omitting registration - these are defined on the same mesh
        #need to get the actual number of cores allocated to the job - different than cpu count
        ntasks = max(_get_allocated_cores() - 1,1)
        transfer = LinearPointCloudMeshTransfer(self.body[self.bounded_body],
                                                data[self.bounded_body])
        scl_sol= transfer(scl_points,k = 16,n_jobs = ntasks)
        if scl_sol.ndim == 1:
            scl_sol = scl_sol[:,np.newaxis]
        
        print(scl_sol.shape)

        return APDLIntegrate(scl_sol,scl_points,npoints)



def read_node_pos_sol_data(data_files: APDLDataFiles) -> pd.DataFrame:
    
    node_loc = read_apdl_node_file(data_files.location_file)
    node_loc.columns = xcols
    node_sol = read_apdl_node_file(data_files.stress_file)
    node_sol.columns = stress_cols       
    node_temp = read_apdl_node_file(data_files.temperature_file).loc[:,[4]]
    node_temp.columns = temperature_cols
    return pd.concat([node_loc,node_sol,node_temp],axis = 1)

def read_section(name: str,
                 delete_input_data = False) -> Tuple[pd.DataFrame,LinearizedSection]:

    data_files = APDLDataFiles(name)
    df = read_node_pos_sol_data(data_files)
    face1 = read_apdl_node_file(data_files.face1_file)
    face2 = read_apdl_node_file(data_files.face2_file)
    if delete_input_data:
        data_files.remove_files()
    
    return df,LinearizedSection(df[xcols].to_numpy(),face1,face2)

def linearize_section(name: str,
                      delete_input_data = False):


    df,linearize_section = read_section(name,delete_input_data = delete_input_data)

    apdl_stress_int = linearize_section.linearize(df[stress_cols].to_numpy())

    data = {}
    data['membrane'] = apdl_stress_int.membrane_stress_vm(averaged = True)
    data['bending'] = apdl_stress_int.bending_stress_vm(averaged = True)
    pdata = apdl_stress_int.linearized_principal_values(averaged = True)
    for i in range(1,4):
        data[f'principal-stress-{i}'] = pdata[:,i- 1]
    
    data['triaxility_factor'] = apdl_stress_int.linearized_triaxiality_factor(averaged = True)
    dloc = (linearize_section.loc1.to_numpy() + linearize_section.loc2.to_numpy())/2
    for i,x in enumerate(xcols):
        data[x] = dloc[:,i]
    
    data['temperature'] = linearize_section.linearize(df['temperature'].values.reshape((-1,1))).thickness_average()
    
    for key,dat in data.items():
        data[key] = dat.squeeze()

    loc = pd.DataFrame(np.concatenate([linearize_section.loc1.to_numpy(),linearize_section.loc2.to_numpy()],axis = 1),
                       index = linearize_section.loc1.index,
                       columns = [x + '-f1' for x in xcols] + [x + '-f2' for x in xcols])
    
    return pd.DataFrame.from_dict(data),df,loc


def post_process_directory(directory: str | Path,
                           file_key: str,
                           cfd_temp_file: str | Path,
                           cfd_interp_temp_file: str | Path,
                           db_name: str = 'transient_db',
                           sections = ['shell','end_cap','outlet'],
                           report_file: str | Path = None,
                           meta_data = {}):

    directory = Path(directory).resolve()
    logger.info(f'data base folder name: {db_name}')

    #set-up simulation database
    db = SimulationDatabase(db_name,
                            create_db= not os.path.exists(db_name))

    #container for post processesd and simulation data
    psd = SimulationData(complevel= 9)

    #iterate through sections and linearize
    for section in sections:
        logger.info(f'linearizing {section} ...')
        pdf,idat,iloc = linearize_section(str(Path(directory).joinpath(section)))
        
        #save linearized data container
        psd[section + '/processed'] = pdf
        psd[section + '/simulation_data'] = idat
        psd[section + '/face_locations'] = iloc
        
        
    #save cfd data to container,delete original files
    fluent_data =  pd.read_csv(cfd_temp_file,index_col= 0,header = 0)
    fluent_data.columns  = [c.strip() for c in fluent_data.columns]
    psd['fluent_data'] = fluent_data
    psd['interpolated_fluent_data'] = pd.read_csv(cfd_interp_temp_file,index_col = 0,header = 0)

    if report_file is not None:
        report_file = Path(report_file).resolve()
        if report_file.exists():
            logger.info(f'report file {str(report_file)} exists, adding to simulation darta')
            rdf = pd.read_csv(str(report_file),index_col=0,header=0,skiprows = 2, sep = r'\s+')
            columns = rdf.columns.tolist()
            columns = [c.strip() for c in rdf.columns]
            rdf.columns = columns
            psd['report_file'] = rdf 
        else:
            logger.warning(f'report file {str(report_file)} does not exist, skipping')

    logger.info('completed linearization, appending post processed data to data base...')
    db.add(file_key,meta_data,psd)
    return True

def main():
    
    path = Path('/storage/scratch1/2/mlanahan3/michael/t-tube/test_new/test_v2023R2')
    post_process_directory(
        path / '0_4/_structural_results_sXrn6NDqYLL75vLu',
        '4',
        path / '0_4/ttube_temperatures.surf.out',
        path / '0_4/interpolated_temperatures_sXrn6NDqYLL75vLu.csv',
        db_name = path / 'test_db', 
        sections = [],
        report_file = path / '0_4/report-file-0.out',
        meta_data = {'hf': 10e6}
    )


if __name__ == '__main__':
    main()