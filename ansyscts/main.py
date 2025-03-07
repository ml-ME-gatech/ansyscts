import logging
import argparse
from ansyscts.events import CFDOutputFileHandler, Runner
from sim_datautil.sim_datautil.dutil import SimulationDatabase
from ansyscts.miscutil import _exit_error
from pathlib import Path
from watchdog.observers.polling import PollingObserver
import datetime
import ansyscts.config as config
import os

logger = logging.getLogger("ansyscts")

def running_job(folder: Path,
                args: argparse.Namespace):
    
    PATH_TO_WATCH = folder.joinpath(args.path_to_watch).resolve()
    if not PATH_TO_WATCH.exists():
        logger.info(f'Path {PATH_TO_WATCH} does not exist, creating')
        PATH_TO_WATCH.mkdir(parents=True)
    elif not PATH_TO_WATCH.is_dir():
        _exit_error(f'Path {PATH_TO_WATCH} exists and is not a directory')
    
    logger.info(f'Watching {PATH_TO_WATCH} for new CFD output files')

    event_handler = CFDOutputFileHandler(PATH_TO_WATCH,
                                         parent = os.getcwd(),
                                         max_workers = config.MAX_WORKERS_)
    observer = PollingObserver()
    observer.schedule(event_handler, str(PATH_TO_WATCH), recursive=True)
    observer.start()

    runner = Runner(event_handler,observer)
    runner.run()

def interrupted_job(folder: Path,
                    args: argparse.Namespace,
                    db_name: Path):

    PATH_TO_WATCH = folder.joinpath(args.path_to_watch).resolve()
    if not PATH_TO_WATCH.exists():
        raise FileNotFoundError(f'Path {PATH_TO_WATCH} does not exist')
    elif not PATH_TO_WATCH.is_dir():
        raise NotADirectoryError(f'{PATH_TO_WATCH} exists and is not a directory')
    
    logger.info(f'Continued process of files in {PATH_TO_WATCH}')


    event_handler = CFDOutputFileHandler(PATH_TO_WATCH,
                                         parent = os.getcwd(),
                                         max_workers = config.MAX_WORKERS_)
    observer = PollingObserver()
    observer.schedule(event_handler, str(PATH_TO_WATCH), recursive=True)
    observer.start()

    runner = Runner(event_handler,observer)
    runner.from_interrupted(SimulationDatabase(db_name))

def main():

    #basic argument parsing and checking
    
    parser = argparse.ArgumentParser()
    parser.add_argument('folder',type = str)
    parser.add_argument('--path_to_watch',type = str,default = 'output',
                        help = '(Relative) Path to watch for new CFD output files')
    parser.add_argument('--rmode',type = str,default = 'continue')
    parser.add_argument('--smode',type = str,default = 'running')
    parser.add_argument('--debug',action = 'store_true',help="Enable debug mode.")
    parser.add_argument('--max_workers',type = int,default = 5,help = 'Maximum number of workers for the thread pool')
    parser.add_argument('--queue',type = str,default = 'inferno',help = 'Queue to submit jobs to')
    parser.add_argument('--account',type = str,default = 'gt-my14-paid',help = 'Account to charge')
    
    args = parser.parse_args()
    assert args.smode in {'running','interrupted'}, 'mode must be either running or interrupted'
    assert args.rmode in {'continue','restart'}, 'mode must be either continue or restart'
    
    config.DEBUG_ = args.debug  
    config.RUN_MODE_ = args.rmode
    config.MAX_WORKERS_ = args.max_workers
    config.QUEUE_ = args.queue
    config.ACCOUNT_ = args.account

    #check if folder exists
    folder = Path(args.folder).resolve()
    if not folder.exists():
        raise FileNotFoundError(f'Folder {folder} does not exist')
    elif not folder.is_dir():
        raise NotADirectoryError(f'{folder} is not a directory')
    
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

    #run the job
    if args.smode == 'running':
        running_job(folder,args)
    else:
        interrupted_job(folder,args)
