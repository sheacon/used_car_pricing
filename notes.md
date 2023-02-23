
# Notes

## Tools

csvkit (https://csvkit.readthedocs.io)

## Data Processing

1. unzip

2. run pyspark script process_listings.py
	- drop unneeded columns
	- replace photo links with count
	- compact listed options and hvf options

3. dedupe listings
	- keep the most recent listing prior to a registration date


## Random

```

### interactive pyspark setup
salloc --time=08:00:00 --mem=32G --cpus-per-task=16 --constraint=skylake
module load GCC/10.2.0 OpenMPI/4.0.5 SciPy-bundle/2020.11 IPython/7.18.1 Spark/2.2.1