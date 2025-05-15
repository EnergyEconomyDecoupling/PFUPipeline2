#!/bin/bash

# Submit the pipeline as a background process with ./run.sh
# module load R # Uncomment if R is an environment module.

# The following line works, but not with >128 connections
# nohup nice -4 R CMD BATCH run.R &

# Fails to apply the correct number of connections.
# R --max-connection=512 CMD BATCH run.R

R --max-connections=512 -f run.R

# Change the nice level above as appropriate
# for your situation and system.

# Removing .RData is recommended.
# rm -f .RData
