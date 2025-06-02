
#config for ansyscts
DEBUG_ = False                  #set to True for debugging
RUN_MODE_ = 'restart'           #set to 'continue' for continuing executation on errors. Set the "restart" to force exit on errors
MAX_WORKERS_ = 5                #maximum number of workers for the thread pool
CHECK_INTERVAL_ = 0.5           #interval to check for file completion in seconds
FILE_WAIT_RETRIES_ = 600        #number of retries to check for file completion
FILE_WAIT_INTERVAL_ = 1.0       #interval to check for file completion in seconds
REPORT_FILE_NAME_ = 'report-file-0.out' #name of the report file
CLUSTER_DELAY_ = 5.0            #delay between job submissions in seconds
DASK_TIMEOUT_ = 3600            #timeout for dask client in seconds
DASK_ALLOWED_FAILURES_ = 5      #number of allowed failures for dask client
MAX_WAIT_TIME_ = 24*60*60       #maximum wait time for script
CFD_INPUT_FILE_DEFAULT_ = 'ttube_temperatures.surf.out'
#config for slurm
ACCOUNT_ = 'gts-my14-paid'      #account to charge
QUEUE_ = 'inferno'              #queue to submit jobs to

#resources for slurm

def slurm_resource_parser(queue: str,
                          account: str,
                          cores: int,
                          memory: str,
                          walltime: str):
    
    return {'queue': queue,
            'account': account,
            'cores': cores,
            'memory': memory,
            'walltime': walltime}

def inferno_slurm_resource(cores: int,
                           memory: str,
                           walltime: str):
    
    return slurm_resource_parser(ACCOUNT_,
                                 QUEUE_,
                                 cores,
                                 memory,
                                 walltime)



