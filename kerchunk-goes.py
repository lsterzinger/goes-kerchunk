ddimport warnings
warnings.filterwarnings('ignore')

import fsspec
import dask.bag as db
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from datetime import datetime
from dask.distributed import LocalCluster


#################################################
# Define year and day (single day only for now) #
#################################################
# year = int(input("Please input a year: "))
# day = int(input("Please enter a day: ")) # April 11, 2020

year = 2020
day = 102 # April 11, 2020

product = "ABI-L2-ACMC"

##############
# SLURM info #
##############

queue = 'high'
ncpu = int(input("How many CPUs per job? (recommend 12): "))
njobs = int(input("How many jobs to submit?: ")) # number of jobs to submit

################
# Do the work! #
################

t1 = datetime.now()

if __name__ == '__main__':
    # Start a cluster on dask (use up a whole node with --exclusive)
    # this is where the actual computing of Kerchunk will happen
    print("Starting dask cluster")
    cluster = SLURMCluster(
        queue = queue,
        walltime = '01:00:00', 
        cores = ncpu,
        n_workers=ncpu,
    )

    cluster.scale(jobs=njobs)
    client = Client(cluster)

    # Set up s3 filesystem
    fs = fsspec.filesystem('s3', anon=True)

    # get list of files for given product, year, day
    print("Getting filelist")
    flist = fs.glob(f"s3://noaa-goes16/{product}/{year}/{day}/*/*.nc")

    print(f"Found {len(flist)} files")


    # Do the parallel computation

    def gen_ref(f):
        with fs.open(f) as inf:
            return SingleHdf5ToZarr(inf, f).translate()

    with cluster:
        with client:
            print("Setting up dask parallelization")
            #print(client)
            bag = db.from_sequence(flist, partition_size=1).map(gen_ref)

            with client:
                print("Kerchunking individual files in parallel")
                dicts = bag.compute()


    # Combine all references into single file and write to disk
    print("Combining into single reference")
    mzz = MultiZarrToZarr(
        dicts,
        concat_dims='t',
        inline_threshold=0,
        remote_protocol='s3',
        remote_options={'anon':True}
    )

    mzz.translate(f"{product}-{year}-{day}.json")

    t2 = datetime.now()
    print(f"Done! This script took {(t2 - t1).total_seconds()} seconds")