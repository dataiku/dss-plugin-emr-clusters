# EMR clusters plugin (emr-clusters)
Plugin to add AWS EMR support in Dataiku DSS

## Contributors
- Dataiku ([dataiku](https://github.com/dataiku))
- Dennis P. Michalopoulos ([dmichalopoulos](https://github.com/dmichalopoulos))

## Version
0.0.3

## Documentation
https://doc.dataiku.com/dss/latest/hadoop/dynamic-emr.html

## License
This project is licensed under the Apache Software License.

## Release Notes (v0.0.3)

### New macro: `emr-copy-cluster`
- Allows you to launch a new cluster whose settings are copied from an existing cluster
- Copyable clusters are limited to those created by the `emr-create-cluster` macro

### New cluster parameter configurations added to `emr-create-cluster`
- Add your own EMR software application configurations
- Request spot instances for CORE and TASK nodes at a specified bid price (defaults to on-demand price)
- Select which software applications you want to install (from a limited set)
- Specify desired number of EBS volumes per instance group
- Specify whether or not to use EBS-optimized volumes (applies to all groups)
- Add termination protection to your cluster (turned off automatically when stopping a created/copied cluster)

### Various code refactorings
- Moved core logic of `Cluster.start(..)` and `Cluster.stop(..)` functions to `cluster_ops.py`
- Created `boto_params.py` to store class-level variables for all required `boto3` client commands and responses
- Some rewording of parameter descriptions

### In development (not yet implemented)
- Allow user to specify python libraries to install on each cluster node
- Allow user to specify shell commands (e.g., to create directories or perform other custom actions) to run on each instance once cluster launch is complete
- Make it such that both the above are performed on any new instances added after rescaling operations, or when spot instances are terminated and replaced

