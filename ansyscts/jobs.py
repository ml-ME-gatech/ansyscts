from abc import ABC,abstractmethod
from abc import abstractmethod
from typing import Callable
#cluster and distributed/async computing
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from asyncio import TimeoutError,CancelledError
import subprocess
from tempfile import TemporaryDirectory
import shutil
import os
from typing import Tuple, Dict, Optional
from pathlib import Path
from sciterp.mesh_transfer import LinearPointCloudMeshTransfer

from ansyscts.miscutil import _safe_read_csv_file, _safe_file_copy, _try_to_delete_file,_get_allocated_cores
from ansyscts.post import post_process_directory
import ansyscts.config as config
import datetime
import ansyscts.senv as senv
import logging
import pandas as pd

logger = logging.getLogger("ansyscts")

_PARENT = Path(os.getcwd())

#will need to setup this up in a config file
TTUBE_NODE_FILE = _PARENT.joinpath(senv.TTUBE_NODE_FILE_)
TTUBE_DAT_FILE = _PARENT.joinpath(senv.TTUBE_DAT_FILE_)
APDL_SCRIPTS_FOLDER = Path(__file__).parent.parent.resolve().joinpath('apdl_scripts')

class SlurmJob(ABC):

    _defaults = {}
    def __init__(self,name: str,
                      client: Client = None,
                      cluster: SLURMCluster = None,
                      **resource_kwargs):
         
        self.name = name
        self.client = client
        self.cluster = cluster
        self.future = None
        self.resource_kwargs = resource_kwargs
        self.parse_resource_kwargs()
    
    def __str__(self):
        return self.name + '_' + str(self.client) + '_' + str(self.cluster)

    @abstractmethod
    def make_client(self): 
        pass

    @abstractmethod
    def run(self):
        pass
    
    def _run(self, func: Callable,*args, **kwargs):
        if not self.client:
            self.make_client()
        
        self.client.wait_for_workers(1)                     #block until workers become available
        self.future = self.client.submit(func, *args, **kwargs)  #submit job,this blocks until the result of the job completes
        
        try:
            results = self.future.result()                       #get the results
        except (TimeoutError,CancelledError) as e:
            logger.error(f"Job {self.name} timeout out or was cancelled {e}")
            results = False
        except Exception as e:
            logger.error(f"Error in job {self.name}: {e}")
            results = False
        finally:
            self.kill()
        
        return results                              
    
    def kill(self):
        try:
            if self.client and self.future:
                self.client.cancel(self.future,force = True)
                self.client.close()
            if self.cluster:
                self.cluster.close()
        except Exception as e:
            if 'IOLoop is closed' in str(e):
                logger.debug(f"Cluster/Client already closed for {self.name}: {e}")
            else:
                logger.error(f"Error during killing jobs for {self.name}: {e}")

    def parse_resource_kwargs(self):
        for key,value in self._defaults.items():
            if key not in self.resource_kwargs:
                self.resource_kwargs[key] = value
        
        self.resource_kwargs['account'] = config.ACCOUNT_  
        self.resource_kwargs['queue'] = config.QUEUE_

        if config.DEBUG_:
            kwargs_str = '\n'.join([f'{k}: {v}' for k,v in self.resource_kwargs.items()])
            logging.info(f"Resource kwargs for {self.name}: \n {kwargs_str}")


def make_new_slurm_cluster_client(name: str,
                                  queue = 'inferno',
                                  account = 'gts-my14',   
                                  cores = 24,
                                  memory = '64GB',
                                  walltime = '01:00:00',
                                  additional_directives = [],
                                 **kwargs) -> Tuple[SLURMCluster,Client]:
    
    """"
    make a new slurm cluster
    """
    extra_directives = [
    f'#SBATCH--ntasks-per-node={cores}',
    '#SBATCH  -opace.out-%j.out',
   f'#SBATCH  -J{name}',
    ] + additional_directives

    cluster = SLURMCluster(
        queue=queue,
        account = account,
        cores= cores,
        memory= memory,
        walltime=walltime,
        job_extra_directives= extra_directives
    )

    cluster.scale(1)
    return cluster,Client(cluster,**kwargs)

def save_apdl_outputs(temp_dir: Path,
                      new_dir: Path):
    
    for file in Path(temp_dir).iterdir():
        if '.node.loc' in file.name or '.node.dat' in file.name or '.node.cfdtemp' in file.name:
            _safe_file_copy(file, new_dir)

def run_apdl_shell_command(temp_dir: Path,
                           result_dir: Path,
                           *args, 
                           ansys_version = '2023R2',
                           **kwargs):
    
    acmd = ansys_version[2:4] + ansys_version[-1]
    """
    Run ANSYS APDL shell command
    """
    # Change python (vs. slurm vs. APDL) directory programmatically
    curr_dir = os.getcwd()
    os.chdir(str(temp_dir))  

    # Build the command string (or list)
    ntasks = _get_allocated_cores() - 4  # Reserve 4 cores for other tasks
    cmds = [f'module load ansys/{ansys_version}',
            f"ansys{acmd} -s noread -smp -np {ntasks} -b < export_data_ttube.input > apdl.out 2>&1"]
    
    result = subprocess.run('\n'.join(cmds), shell=True, cwd = str(temp_dir),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,text = True)
    if result.returncode != 0:
        logger.error(f'Running APDL command failed with return code {result.returncode}')
        logger.error(f"stderr:  {result.stderr}")
        logger.error(f"stdout:  {result.stdout}")
    
    save_apdl_outputs(temp_dir, result_dir)
    os.chdir(curr_dir)  # Change back to the original directory
    return result.returncode == 0

def preprocess_cfd_output(cfd_input_file: Path, 
                          outputfile : Path):
    
    
    if config.DEBUG_:
        logger.info(f'Preprocessing CFD output file: {cfd_input_file}')
        logger.info(f'Output file will be saved to: {outputfile}')
    #Read the cfd output
    cfd_df = _safe_read_csv_file(cfd_input_file, index_col=0, header=0, sep=',')
    if cfd_df is None:
        return False
    cfd_df.columns = [c.strip() for c in cfd_df.columns]

    if config.DEBUG_:
        logger.info(f'CFD DataFrame shape: {cfd_df.shape}')
        logger.info(f'CFD DataFrame columns: {cfd_df.columns.tolist()}')
    
    #avoid passing to much data to dask, just read it on the node
    ttude_nodes = _safe_read_csv_file(TTUBE_NODE_FILE,index_col = 0,header = None,sep = ',')
    if ttude_nodes is None:
        raise FileNotFoundError(f"Could not read the file: {TTUBE_NODE_FILE}")
    
    if config.DEBUG_:
        logger.info(f'T-tube Nodes DataFrame shape: {ttude_nodes.shape}')
        logger.info('Transferring temperature data from CFD output to T-tube nodes')

    xcols = [c + '-coordinate' for c in ['x','y','z']]
    transfer = LinearPointCloudMeshTransfer(cfd_df[xcols].values,
                                            cfd_df[['temperature']].values)
    
    #need to get the actual number of cores allocated to the job - different than cpu count
    ntasks = max(_get_allocated_cores() - 4,1)
    if config.DEBUG_:
        logger.debug(f'Number of tasks allocated for interpolation: {ntasks}')
    
    temperature_out,translation = transfer(ttude_nodes.values,translate = True,k = 16,n_jobs = ntasks)

    output_df = pd.DataFrame(temperature_out, index=ttude_nodes.index.astype(int), columns=['temperature'])
    if config.DEBUG_:
        logger.info('Completed transferring temperature data from CFD output to T-tube nodes')
        logger.info(f'Output DataFrame shape: {output_df.shape}')
        logger.info(f'Computed Translation: x = {translation[0]},y = {translation[1]}, z = {translation[2]}')

    output_df.to_csv(outputfile)
    return True

class StructuralAnalysisJob(SlurmJob):

    _defaults = {'memory':'128GB','walltime':'04:00:00','cores':24}

    def __init__(self,name: str,
                    client: Client = None,
                    cluster: SLURMCluster = None,
                    parent_dir: Optional[Path] = None,
                    **resource_kwargs):
        
        super().__init__(name,client,cluster,**resource_kwargs)
        parent_dir = Path(os.getcwd()) if parent_dir is None else parent_dir
        self.dir = TemporaryDirectory(dir=str(parent_dir))

    def make_client(self):
        additional_directives = [
            f'#SBATCH -D {self.dir.name}',  # Set working directory, APDL is super messy, I don't want to clutter up the main directory
        ]
        
        self.cluster,self.client = make_new_slurm_cluster_client(self.name,
                                                    additional_directives = additional_directives,
                                                    **self.resource_kwargs)
    
    def run(self,   interpolated_temperature_file: Path,
                    result_path: Path) -> bool:
        
        _temp_path = Path(self.dir.name).resolve()
        if not _temp_path.exists():
            _temp_path.mkdir(parents=True)
        
        if not result_path.exists():
            result_path.mkdir(parents=True)
        
        if not APDL_SCRIPTS_FOLDER.exists():
            logger.error(f"APDL scripts folder {str(APDL_SCRIPTS_FOLDER)} does not exist. Cannot proceed with structural analysis")
            return False
        
        if not TTUBE_DAT_FILE.exists():
            logger.error(f"TTUBE data file {str(TTUBE_DAT_FILE)} does not exist. Cannot proceed with structural analysis")
            return False
        
        #copy the interpolated temperature file to the temp directory
        if not _safe_file_copy(interpolated_temperature_file,_temp_path.joinpath('interpolated_temperatures.csv')):
            logger.error(f'Could not copy interpolated temperature file {str(interpolated_temperature_file)} to temp directory')
            return False
        
        #copy the Tube data file to the temp directory
        if not _safe_file_copy(TTUBE_DAT_FILE,_temp_path.joinpath('ttube_half.dat')):
            logger.error(f'Could not copy TTUBE data file {str(TTUBE_DAT_FILE)} to temp directory')
            return False
        
        #copy the files to the temp directory to run in a local directory.
        for file in APDL_SCRIPTS_FOLDER.iterdir():
            if not _safe_file_copy(file,_temp_path):
                logger.error(f'Could not copy APDL script file {str(file)} to temp directory')
                return False
        
        #run the analysis
        if config.DEBUG_:
            logger.info(f'Running structural analysis in temp directory: {str(_temp_path)}')
            logger.info(f'Writing to result path: {str(result_path)}')
        
        success = self._run(run_apdl_shell_command,_temp_path,result_path)
        
        #clean up the temp directory if process executed normally, otherwise, need to save
        if success and not config.DEBUG_:
            self.dir.cleanup()
        elif not success:
            logger.error(f'Structural analysis job {self.name} failed. Keeping temp directory for debugging: {str(_temp_path)}')
        
        return success
    
class PreProcessCFDOutputJob(SlurmJob):

    _defaults = {'memory':'32GB','walltime':'00:20:00','cores':24}

    def make_client(self):
        self.cluster,self.client = make_new_slurm_cluster_client(self.name,
                                                    **self.resource_kwargs)
        
        
    def run(self, cfd_input_file: Path, outputfile: Path) -> bool:    
        return self._run(preprocess_cfd_output,cfd_input_file, outputfile)

class PostProcess(SlurmJob):

    _defaults = {'memory':'64GB','walltime':'02:00:00','cores':24}

    def make_client(self):
        self.cluster,self.client = make_new_slurm_cluster_client(self.name,
                                                    **self.resource_kwargs)
    
    def get_flow_time_from_report_file(self,report_file: Path,
                                            time_step: int) -> int:
        df = _safe_read_csv_file(report_file, header = None,skiprows=3,sep = r'\s+')
        with open(report_file,'r') as f:
            for line in f.readlines():
                if '("Time Step"' in line:
                    columns = line.strip()[1:-1].split('"')
                    columns = [col.replace('"','').strip() for col in columns]
                    columns = [col for col in columns if col]
                    break

        df.columns = columns
        df.set_index('Time Step', inplace=True) 
        return df.loc[time_step,'flow-time']
    
    def run(self,  structural_results_folder: Path,
                   cfd_output_file: Path,
                   interpolated_temperature_file: Path,
                   time_step: int | None,
                   db_name: str | Path,
                   report_file: Path,
                   delete_intermediate_files: bool = False,
                   meta: Dict ={},
                   file_key: str = '') -> bool:
        
        
        if time_step is not None:
            flow_time = self.get_flow_time_from_report_file(report_file,time_step)
            meta.update({'flow_time': float(flow_time), 'time_step': time_step})
        else:
            flow_time = None
        
        file_key = str(time_step) if time_step is not None else file_key
        if config.DEBUG_:
            logger.info(f'Post-processing structural results for file key: {file_key}')
            logger.info(f'Structural results folder: {structural_results_folder}')
            logger.info(f'CFD output file: {cfd_output_file}')
            logger.info(f'Database name: {db_name}')
        
        post =  self._run(post_process_directory,structural_results_folder,file_key,
                         cfd_output_file,
                         interpolated_temperature_file,
                         db_name = db_name,
                         meta_data = meta,
                         report_file = report_file)
        
        if post:
            if delete_intermediate_files:
                logger.info('Succesful post-processing completed: deleting intermediate files')
                _try_to_delete_file(cfd_output_file)
                _try_to_delete_file(interpolated_temperature_file)
                shutil.rmtree(structural_results_folder)
        else:
            logger.error(f'Post processing of {str(structural_results_folder)} failed - did not delete files for debugging purposes')
        
        return post