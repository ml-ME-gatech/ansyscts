
#config for ansyscts
DEBUG_ = False
RUN_MODE_ = 'restart'
MAX_WORKERS_ = 5

#config for slurm
ACCOUNT_ = 'gt-my14-paid'
QUEUE_ = 'inferno'

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



