#!/bin/bash

for detr in 0 1; do
    for il in {0..10}; do
        qsub -v detr=$detr,il=$il -N mme_${il}_${detr} -o out_mme_${il}_${detr}.txt -e err_mme_${il}_${detr}.txt run_mmeprob.pbs
    done
done
