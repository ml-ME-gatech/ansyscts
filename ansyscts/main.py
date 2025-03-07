import logging
import argparse
from ansyscts.events import CFDOutputFileHandler, Runner
from sim_datautil.sim_datautil.dutil import SimulationDatabase
from ansyscts.miscutil import _exit_error
from pathlib import Path
from watchdog.observers.polling import PollingObserver
import datetime

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

    event_handler = CFDOutputFileHandler(PATH_TO_WATCH)
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


    event_handler = CFDOutputFileHandler(PATH_TO_WATCH)
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
    parser.add_argument('--mode',type = str,default = 'running')
    
    args = parser.parse_args()
    assert args.mode in {'running','interrupted'}, 'mode must be either running or interrupted'

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
    logger.info(f'Starting coupled CFD-Structural simulation in folder {folder} in mode: {args.mode}')

    #run the job
    if args.mode == 'running':
        running_job(folder,args)
    else:
        interrupted_job(folder,args)
