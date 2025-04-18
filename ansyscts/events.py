from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import signal
import time
import sys

from sim_datautil.sim_datautil.dutil import SimulationDatabase
from ansyscts.jobs import PreProcessCFDOutputJob, StructuralAnalysisJob, PostProcess, SlurmJob
from ansyscts.miscutil import _parse_fluent_output_filename, _is_file_complete, _safe_file_copy, _exit_error
import logging
import ansyscts.config as config

logger = logging.getLogger("ansyscts")

class CFDOutputFileHandler(FileSystemEventHandler):

    def __init__(self,folder: Path,
                      db_name: str | Path,
                      parent: Path = None,
                      max_workers: int = 5):
        
        super().__init__()
        self.executor = ThreadPoolExecutor(max_workers)
        self.folder = folder
        self.running_jobs = {}
        self.parent = self.folder.parent if parent is None else parent
        self.db_name = db_name  
    
    def on_created(self, event):
        file = Path(event.src_path)
        self.executor.submit(self.process_file,file)
    
    def run(self, job: SlurmJob,
                  *args,
                  **kwargs):
        
        key = str(job)
        self.running_jobs[key] = job
        results = False
        try:
            results = job.run(*args,**kwargs)
        except Exception as e:
            self.error_process(f"Error running job {job}: {str(e)}")
        finally: 
            self.running_jobs.pop(key,None)
        
        return results
    
    def error_process(self, msg: str):
        if config.RUN_MODE_ == 'restart':
            _exit_error(msg)
        else:
            logger.error(msg)
        
    def process_file(self, file: Path):
        
        results_folder = Path(self.parent)
        if not results_folder.exists():
            results_folder.mkdir(parents=True)
        else:
            if not results_folder.is_dir():
                _exit_error(f'Path {results_folder} exists and is not a directory')

        if self._is_cfd_file(file):
            logger.info(f"New CFD output file detected: {file}")
            if _is_file_complete(file):
                logger.info(f"File {file} determined completed, proceeding with analysis")

                #Pre-process the CFD output
                time_step = _parse_fluent_output_filename(file)
                if time_step is None:
                    self.error_process(f"Could not parse time step from file name {file}")
                
                logger.info('Pre-processing cfd inputs')
                interp_file = file.parent.joinpath(f'interpolated_temperatures_{time_step}.csv')
                cfd_pre = PreProcessCFDOutputJob('cfd_pre - '+file.stem)  
                if not self.run(cfd_pre,file,interp_file):
                    self.error_process(f"Pre-processing of cfd file {file} failed")
                
                #Run Structural Analysis
                logger.info('Running structural analysis')
                structrual = StructuralAnalysisJob('structural - '+file.stem,
                                                   parent_dir = self.parent)
                
                struct_results_folder = results_folder.joinpath(file.stem + '_structural_results')
                if not self.run(structrual,interp_file,struct_results_folder):
                    self.error_process(f"Structural analysis of cfd file {file} failed")

                _safe_file_copy(file,struct_results_folder)
                #Post Processing
                logger.info('Post-processing structural results')
                post_process = PostProcess('post_process - '+file.stem)
                rfile = self.parent.joinpath(config.REPORT_FILE_NAME_)
                if not self.run(post_process,struct_results_folder,file,interp_file,time_step,self.db_name,rfile):
                    self.error_process(f"Post-processing of structural file {file} failed")
                
                logger.info(f"Analysis of {file} completed successfully")
                
            else:
                logger.warning(f"File {file} not completed in time, skipping analysis")

    def _is_cfd_file(self,file: Path) -> bool:
        if not file.is_dir() and file.suffix == '.out' and 'temperature' in file.stem:
            return True

    def shutdown(self):
        logger.info('Killing running jobs')
        for name,job in self.running_jobs.items():
            try:
                job.kill()
                logger.info(f"Killed job {name}")
            except Exception as e:
                logger.error(f"Error killing job {name}: {str(e)}")

        self.executor.shutdown(wait=False)
        logger.info('Shut down CFD output file handler')

    def from_interrupted(self, db: SimulationDatabase):
        """
        Restart the CFDOutputFileHandler from an interrupted state
        """
        if self.executor._shutdown:
            logger.error("Executor is already shut down; cannot restart interrupted jobs.")
            return
    
        logger.info('Restarting CFDOutputFileHandler from interrupted state')
        for file in self.folder.iterdir():
            time_step = _parse_fluent_output_filename(file)
            if str(time_step) not in db.keys() and self._is_cfd_file(file):
                logger.info(f"Restarting analysis of file {file}")
                try:
                    self.executor.submit(self.process_file,file)
                    time.sleep(config.CLUSTER_DELAY_)       #wait a bit after starting job
                except Exception as e:
                    logger.error(f"Error submitting job for file {file}: {str(e)}")

class Runner:

    def __init__(self,event_handler: CFDOutputFileHandler,
                      observer: PollingObserver):
        self.event_handler = event_handler
        self.observer = observer    

        signal.signal(signal.SIGINT,self.termination_handler)
        signal.signal(signal.SIGTERM,self.termination_handler)

    def termination_handler(self,signal_received,frame):
        logger.info(f"Received shutdown signal ({signal_received}). Initiating graceful shutdown.")
        self.event_handler.shutdown()  # Shutdown ThreadPool and running jobs
        self.observer.stop()           # Stop the filesystem observer
        # Optionally wait for the observer to finish if needed
        self.observer.join()
        sys.exit(0)
        
    def run(self, wait: float = config.CLUSTER_DELAY_):
        try:
            while True:
                time.sleep(wait)
        except KeyboardInterrupt:
            logger.info('Keyboard interrupt received, stopping observer')
            self.termination_handler(signal.SIGINT, None)
        
        self.observer.join()
        self.event_handler.shutdown()

    def from_interrupted(self, db: SimulationDatabase):
        try:
            self.event_handler.from_interrupted(db)
            self.event_handler.executor.shutdown(wait=True)
            self.observer.join()
        except KeyboardInterrupt:
            logger.info('Keyboard interrupt received, stopping execution for interrupted files')
            self.termination_handler(signal.SIGINT, None) 


