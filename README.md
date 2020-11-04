# Pacemaker + HDFS

_This is an active repository. Code for OSDI 2020 experiments has been tagged and released as v1.0_

## HDFS cluster requirement
We have written this README based on our experiments conducted on the CloudLab infrastructure using the APT cluster and r320 nodes. All the setup scripts expect that the same cluster is re-used for the evaluation of our HDFS + PREACT artifact. We have confirmed with the OSDI artifact evaluation committee that usage of Cloudlab can be assumed for our experiment evaluation. If a different cluster is required, please contact saukad@cs.cmu.edu.


## Setting up the Cloudlab experiment
Instantiate a Cloudlab experiment using the following profile: `osdi-preact-artifact-eval`. This will spawn an experiment with 42 nodes. 21 of those nodes will be used for HDFS, whereas 21 nodes will be used for dfs-perf, which is the workload generator for our evaluation. dfs-perf instructions will be provided after HDFS has been setup.

You will require the private key (of the public key you used to ssh into the Cloudlab machine -- usually is the `~/.ssh/id_rsa` file on your local computer on Linux and Mac platforms) to be copied to `~/.ssh/` path of `node0`.


## Cloning the repository in node0
Once the experiment has been instantiated, you can login into node0 and clone the repository in the directory of your choice. When testing, we used the shared NFS folder `/proj/<project>/` because it had more space than the user's home directory. But, you could choose any directory to clone the repository. The entire setup of the HDFS cluster will happen from the node0, which will also be the Namenode for the HDFS cluster that will be setup.


## Setting up the Cloudlab experiment
There are several steps involved in setting up the HDFS cluster to evaluate our experiment.
1. `cd hdfs` in our current repository
2. run `bash run.sh`


## DFS-perf configuration
Once the HDFS cluster has been setup, please refer to the [README](../dfs-perf/README.md) in the dfs-perf folder to run the workload and generate the evaluation numbers.


## Reference
This HDFS port is a part of the experiments conducted in [PACEMAKER: Avoiding HeART attacks in storage clusters with disk-adaptive redundancy](https://www.usenix.org/conference/osdi20/presentation/kadekodi) published at USENIX OSDI 2020.

## Contact
Saurabh Kadekodi: saukad@cs.cmu.edu
