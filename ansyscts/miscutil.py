from pathlib import Path
import pandas as pd
import time
import sys
import shutil
import os
import logging
import ansyscts.config as config

logger = logging.getLogger("ansyscts")

def _try_to_delete_file(file: str): 
    try:
        os.remove(file)
        logger.info(f'{file} deleted')
    except (FileNotFoundError,FileExistsError) as fe:
        logger.warning(str(fe))

def _is_file_complete(file_path: Path,
                      check_interval = config.FILE_WAIT_INTERVAL_,
                      retries = config.FILE_WAIT_RETRIES_) -> bool:
    
    #helper/checker file to make sure that the file is not being written to
    #while we are trying to read it
    file = Path(file_path)

    previous_size = -1
    for _ in range(retries):
        try:
            current_size = file.stat().st_size  
        except OSError:
            # The file might not exist yet or be inaccessible.
            return False
        
        if current_size != previous_size:
            previous_size = current_size
            time.sleep(check_interval)
        else:
            return True
    return False

def _safe_read_csv_file(file_path: Path,**kwargs) -> pd.DataFrame:
    """
    "Safely" read a csv file, i.e. check if theres any common errors on reading 
    and log if something comes up
    """
    try:
        df = pd.read_csv(file_path,**kwargs)
        return df
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}. Error: {e}")
        return None
    except pd.errors.ParserError as e:
        logger.error(f"Parsing error for file: {file_path}. Error: {e}")
        return None

def _safe_file_copy(src: Path, dst: Path) -> bool:
    """
    "Safely" copy a file, i.e. check if the file already exists or the source file 
     isnt there, and log if something comes up
    """

    try:
        shutil.copy2(src, dst)
        return True
    except FileExistsError as e:
        logger.error(f"File already exists: {dst}. Error: {e}")
        return False
    except FileNotFoundError as e:
        logger.error(f"Source file not found: {src}. Error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error copying file from {src} to {dst}. Error: {e}")
        return False

def _parse_fluent_output_filename(file: Path) -> int:
    """
    excepting the fluent output file to be:
    "*temperature_{time_step_no}.out"
    """

    if not file.is_file() or file.suffix != '.out' or 'temperature' not in file.stem:
        logger.error(f"File {file} is not a valid fluent output file")
        return None

    try:
        time_step = int(file.stem.split('_')[-1].split('.')[0]) 
        return time_step
    except ValueError as e:
        logger.error(f"Could not parse time step from file name {file}. Error: {e}")
        return None
    
def _exit_error(message: str):
    logger.error(message)
    sys.exit(1)

def _get_allocated_cores() -> int:
    """
    Return the number of CPUs that Slurm allocated to this job
    (falling back to os.cpu_count() if no Slurm variable is set).
    """
    # 1) First check for SLURM_CPUS_ON_NODE
    cpus_on_node = os.environ.get("SLURM_CPUS_ON_NODE")
    if cpus_on_node is not None:
        try:
            return int(cpus_on_node)
        except ValueError:
            logger.warning("SLURM_CPUS_ON_NODE is set but not an integer: "
                           f"{cpus_on_node!r}")

    # 2) Then check for SLURM_CPUS_PER_TASK
    cpus_per_task = os.environ.get("SLURM_CPUS_PER_TASK")
    if cpus_per_task is not None:
        try:
            return int(cpus_per_task)
        except ValueError:
            logger.warning("SLURM_CPUS_PER_TASK is set but not an integer: "
                           f"{cpus_per_task!r}")

    # 3) If none of the Slurm vars exist, fall back to os.cpu_count()
    try:
        return os.cpu_count() or 1
    except Exception as e:
        logger.error(f"Error getting os.cpu_count(): {e}")
        return 1