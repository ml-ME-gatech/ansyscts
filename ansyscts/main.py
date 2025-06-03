import time
start_import = time.time()
import argparse
import ansyscts.config as config
import ansyscts.senv as senv
import logging
from pathlib import Path
import os
import datetime
from typing import Dict, Tuple, Any
import pandas as pd
import copy
from multiprocessing import Process
import signal
import sys
from sim_datautil.sim_datautil.dutil import SimulationDatabase

#basic argument parsing and checking
parser = argparse.ArgumentParser()
parser.add_argument('folder',type = str)
parser.add_argument('--path_to_watch',type = str,default = 'output',
                    help = '(Relative) Path to watch for new CFD output files')
parser.add_argument('--db_name',type = str,default = None)
parser.add_argument('--rmode',type = str,default = 'continue')
parser.add_argument('--smode',type = str,default = 'running')
parser.add_argument('--batch',type = bool,default = False)
parser.add_argument('--debug',action = 'store_true',help="Enable debug mode.")
parser.add_argument('--max_workers',type = int,default = 5,help = 'Maximum number of workers for the thread pool')
parser.add_argument('--queue',type = str,default = 'inferno',help = 'Queue to submit jobs to')
parser.add_argument('--account',type = str,default = 'gts-my14-paid',help = 'Account to charge')
parser.add_argument('--flrfile',type = str,default = 'report-file-0.out',
                    help = 'name of the the fluent report file')
parser.add_argument('--dask_timeout',type = str,default = 3600,
                    help = 'Timeout for dask client in seconds')
parser.add_argument('--dask_allowed_failures',type = int,default = 5)
parser.add_argument('--fluent_report_file',type = str,default = 'report-file-0.out',
                    help = 'Name of the fluent report file')
parser.add_argument('--cfd_input',type = str,default = 'ttube_temperatures.surf.out')
parser.add_argument('--overwrite_db',action = 'store_true',help="overwrite data in the database if it exists")
parser.add_argument('--max_wait',type = int,default = 24*60*60)
parser.add_argument('--post_only',action = 'store_true',
                    help = 'Only run post-processing, do not run the CFD job')

args = parser.parse_args()
assert args.smode in {'running','interrupted'}, 'mode must be either running or interrupted'
assert args.rmode in {'continue','restart'}, 'mode must be either continue or restart'

config.DEBUG_ = args.debug  
config.RUN_MODE_ = args.rmode
config.MAX_WORKERS_ = args.max_workers
config.QUEUE_ = args.queue
config.ACCOUNT_ = args.account
config.REPORT_FILE_NAME_ = args.flrfile
config.DASK_TIMEOUT_ = args.dask_timeout
config.DASK_ALLOWED_FAILURES_ = args.dask_allowed_failures
config.REPORT_FILE_NAME_ = args.fluent_report_file
config.MAX_WAIT_TIME_ = args.max_wait
config.CFD_INPUT_FILE_DEFAULT_ = args.cfd_input

logger = logging.getLogger("ansyscts")
folder = Path(args.folder).resolve()
if not folder.exists():
    raise FileNotFoundError(f'Folder {folder} does not exist')
elif not folder.is_dir():
    raise NotADirectoryError(f'{folder} is not a directory')

if args.db_name is None:
    args.db_name = Path(os.getcwd()).joinpath('transient_db').resolve()

#setup logging
timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
logging.basicConfig(filename = str(folder.joinpath(f'ansyscts-{timestamp}.log')),level = logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger.info(f'Starting coupled CFD-Structural simulation in folder {folder}')
if config.DEBUG_:
    logger.info('Debugging active')


logger.info(f'Run mode: {config.RUN_MODE_}')   
logger.info(f'Simulation mode: {args.smode}') 
logger.info(f'Maximum wait time: {config.MAX_WAIT_TIME_} seconds')

from dask import config as dask_config
timeout_config = ["distributed.scheduler.idle-timeout",
                "distributed.scheduler.no-workers-timeout",
                "distributed.comm.timeout.connect",
                "distributed.comm.timeouts.tcp",
                "distributed.deploy.lost-worker-timeout"]
for toc in timeout_config:
    dask_config.set({toc: config.DASK_TIMEOUT_})

if config.DEBUG_:
    logger.info('Dask timeout configuration:')
    for toc in timeout_config:
        logger.info(f'{toc}:{dask_config.get(toc)}')

dask_config.set({"distributed.scheduler.allowed-failures": config.DASK_ALLOWED_FAILURES_})

from ansyscts.events import CFDOutputFileHandler,CFDOutputProcessor, Runner, ProcessRunner,PostProcess
from sim_datautil.sim_datautil.dutil import SimulationDatabase
from ansyscts.miscutil import _exit_error
from watchdog.observers.polling import PollingObserver

if config.DEBUG_:
    logger.info(f'Finished importing modules: {round(time.time()-start_import,1)} seconds')

def running_job(folder: Path,
                args: argparse.Namespace,
                simulation_type: str = 'transient',
                parent: Path = None,
                meta: Dict = None):
    
    PATH_TO_WATCH = folder.joinpath(args.path_to_watch).resolve()
    if not PATH_TO_WATCH.exists():
        logger.info(f'Path {PATH_TO_WATCH} does not exist, creating')
        PATH_TO_WATCH.mkdir(parents=True)
    elif not PATH_TO_WATCH.is_dir():
        _exit_error(f'Path {PATH_TO_WATCH} exists and is not a directory')
    
    logger.info(f'Watching {PATH_TO_WATCH} for new CFD output files')

    event_handler = CFDOutputFileHandler(PATH_TO_WATCH,
                                         args.db_name,
                                         parent = folder if parent is None else parent,
                                         max_workers = config.MAX_WORKERS_,
                                         sim_type = simulation_type,
                                         meta = meta)
    observer = PollingObserver()
    observer.schedule(event_handler, str(PATH_TO_WATCH), recursive=True)
    observer.start()

    runner = Runner(event_handler,observer)
    runner.run()

def interrupted_job(folder: Path,
                    args: argparse.Namespace):

    PATH_TO_WATCH = folder.joinpath(args.path_to_watch).resolve()
    if not PATH_TO_WATCH.exists():
        raise FileNotFoundError(f'Path {PATH_TO_WATCH} does not exist')
    elif not PATH_TO_WATCH.is_dir():
        raise NotADirectoryError(f'{PATH_TO_WATCH} exists and is not a directory')
    
    logger.info(f'Continued process of files in {PATH_TO_WATCH}')


    event_handler = CFDOutputFileHandler(PATH_TO_WATCH,
                                         args.db_name,
                                         parent = folder,
                                         max_workers = config.MAX_WORKERS_)
    observer = PollingObserver()
    observer.schedule(event_handler, str(PATH_TO_WATCH), recursive=True)
    observer.start()

    runner = Runner(event_handler,observer)
    create = False
    if not os.path.exists(args.db_name):
        logger.warning(f'Database {args.db_name} does not exist, creating')
        create = True
    
    runner.from_interrupted(SimulationDatabase(args.db_name, create_db = create))

def running_batch_job(folder: Path,
                      args: argparse.Namespace):
    """
    This function is used to run a batch job in the running mode.
    It will create a new folder for the job and run the job in that folder.
    """
    parameters = pd.read_csv(folder.joinpath('parameters.csv'), index_col=0, header=0)

    procs = []
    for job_folder in parameters.index:
        meta = parameters.loc[job_folder].to_dict()
        args_ = copy.deepcopy(args)
        args_.path_to_watch = job_folder
        p = Process(target = running_job,
                   args = (folder,args_,'steady-state',folder,meta),
                   daemon = True)
        p.start()
        procs.append(p)
    
    
    # If you want to intercept Ctrl+C in the parent and kill children:
    def handle_sigint(signum, frame):
        logger.info("Batch run got SIGINT → terminating children ...")
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.join()
        exit(0)

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)
    while True:
        time.sleep(config.MAX_WAIT_TIME_)  # Sleep for 24 hours, or adjust as needed

def make_process_file(args: argparse.Namespace,
                      sim_folder: Path,
                      meta: Dict) -> ProcessRunner:

    
    cp = CFDOutputProcessor(sim_folder,
                            args.db_name,
                            parent = sim_folder,
                            sim_type = 'steady-state',
                            meta = meta)
    
    return ProcessRunner(cp)

def make_post_process_folder(args: argparse.Namespace,
                             sim_folder: Path,
                             meta: Dict) -> Tuple[ProcessRunner,Tuple[Any]]:
    """
    This function is used to create a post-processing file.
    It will create a new folder for the job and run the job in that folder.
    """
    post = PostProcess(f'restarted-post-{sim_folder.name}')
    files = {'structural_results': None,
             'cfd_output': None,
             'cfd_interpolatted':None}
    
    for file in sim_folder.iterdir():
        if file.is_dir() and 'structural_results_' in file.name:
            files['structural_results'] = file
        elif file.is_file() and  file.name == config.CFD_INPUT_FILE_DEFAULT_:
            files['cfd_output'] = file
        elif file.is_file() and 'interpolated_temperatures_' in file.name:
            files['cfd_interpolatted'] = file

    for key, value in files.items():
        if value is None:
            logger.warning(f'File {key} not found in {sim_folder}, skipping post-processing')
    
    inputs = (files['structural_results'], files['cfd_output'], files['cfd_interpolatted'],
            None,args.db_name, config.REPORT_FILE_NAME_,False,meta,sim_folder.name)
    
    return ProcessRunner(post),inputs

def process_file(file: str | Path,
                args: argparse.Namespace,
                meta: Dict) -> None:
    """
    This function is used to process a single file in the running mode.
    It will create a new folder for the job and run the job in that folder.
    """
    pr = make_process_file(args, meta)
    
    pr.run(Path(file))

    
def process_batch(folder: str | Path,
                  args: argparse.Namespace):
    """
    Process a batch of files in the running mode.
    Creates a new folder for the job and runs the job in that folder.
    """
    folder = Path(folder).resolve()
    sim_folder = folder/ args.db_name
    parameters = pd.read_csv(folder.joinpath('parameters.csv'), index_col=0, header=0)
    if sim_folder.exists() and not args.overwrite_db:
        sim_db = SimulationDatabase(sim_folder, create_db=False)
        keys = sim_db.keys()
        folder_index = list(set([str(k) for k in parameters.index]) - set(keys))
    else:
        folder_index = parameters.index

    procs = []
    running = {}
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}. Terminating all running processes...")
        for p in procs:
            if p.is_alive():
                logger.info(f"Terminating process {p.pid}")
                p.terminate()
        sys.exit(0)

    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    for job_folder in folder_index:
        sim_folder = folder / str(job_folder)
        file = sim_folder / config.CFD_INPUT_FILE_DEFAULT_
        if not file.exists():
            logger.info(f'File {file} does not exist, skipping')
            continue
        else:
            logger.info(f'Found {file}, processing...')
            meta = parameters.loc[int(job_folder)].to_dict()
            args_ = copy.deepcopy(args)
            if args.post_only:
                running[job_folder], input_args = make_post_process_folder(args_, sim_folder, meta)
            else:
                running[job_folder] = make_process_file(args_,sim_folder, meta)
                input_args = (file,)
                
            while True:
                # Count how many processes in procs are still alive
                alive_count = sum(1 for p in procs if p.is_alive())
                if alive_count < config.MAX_WORKERS_:
                    break
                time.sleep(1)  # sleep a bit and re-check

            p = Process(
                target=running[job_folder].run,
                args= input_args,
                daemon=True
            )
            
            p.start()
            procs.append(p)

    # Wait for all processes to complete
    try:
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Terminating all running processes...")
        for p in procs:
            if p.is_alive():
                logger.info(f"Terminating process {p.pid}")
                p.terminate()
        sys.exit(0)


def main():

    #run the job
    
    if args.batch:
        logger.info('Running in batch mode')
        if args.smode == 'running':
            running_batch_job(folder,args)
        else:
            process_batch(folder,args)
        return
    
    else:
        if args.smode == 'running':
            running_job(folder,args)
        else:
            interrupted_job(folder,args)
