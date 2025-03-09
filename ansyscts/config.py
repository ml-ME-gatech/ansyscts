
#config for ansyscts
DEBUG_ = False                  #set to True for debugging
RUN_MODE_ = 'restart'           #set to 'continue' for continuing executation on errors. Set the "restart" to force exit on errors
MAX_WORKERS_ = 5                #maximum number of workers for the thread pool
CHECK_INTERVAL_ = 0.5           #interval to check for file completion in seconds
FILE_WAIT_RETRIES_ = 600        #number of retries to check for file completion
FILE_WAIT_INTERVAL_ = 1.0       #interval to check for file completion in seconds

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



