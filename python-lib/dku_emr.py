import boto3
import copy
import json
import logging
import os
import pwd
import requests
import subprocess
import traceback


def get_client_and_wait(started_cluster_settings):
    """
    Get a Boto client from the :class:`dataikuapi.dss.admin.DSSClusterSettings` of a started DSS cluster

    :returns: tuple (boto client, emr cluster id)
    """

    config = started_cluster_settings.get_raw()["params"]["config"]
    data = started_cluster_settings.get_plugin_data()
    if data is None:
        raise ValueError("No cluster data, is it stopped/detached?")

    region = config.get("awsRegionId") or get_current_region()
    logging.info("creating Boto client for cluster, region=%s", region)
    client = get_emr_client(config, region)

    logging.info("waiting for cluster %s to be running" % data["emrClusterId"])
    waiter = client.get_waiter('cluster_running')
    waiter.wait(ClusterId=data["emrClusterId"])
    logging.info("cluster started")
    return (client, data["emrClusterId"])


def make_cluster_keys_and_data(client, cluster_id, create_user_dir=False, create_databases=None):

    logging.info("looking up cluster %s" % cluster_id)
    master_instance = None
    response = client.list_instances(ClusterId=cluster_id,
                InstanceGroupTypes=['MASTER'],
                InstanceStates=['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING'])
    #raise Exception("ALL INSTANCES: %s"  % json.dumps(response))
    for inst in response['Instances']:
        h = inst['PrivateIpAddress']
        master_instance = h
        logging.info("master instance address: %s" % h)

    # slave_instances_info = []
    # response = client.list_instances(ClusterId=cluster_id,
    #         InstanceGroupTypes=['CORE', 'TASK'],
    #         InstanceStates=['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING'])
    # for inst in response['Instances']:
    #     slave_instances_info.append({"privateIpAddress":  inst["PrivateIpAddress"]})

    # Look for a custom metastore client factory for Glue-based metastore
    metastoreClientFactoryClass = None
    response = client.describe_cluster(ClusterId=cluster_id)
    for conf in response['Cluster']['Configurations']:
        if conf['Classification'] == "hive-site":
            for k, v in conf['Properties'].iteritems():
                if k == "hive.metastore.client.factory.class":
                    metastoreClientFactoryClass = v

    hadoop_keys = {
        "extraConf" : [
           {"key": "fs.defaultFS", "value" : "hdfs://%s:8020" % master_instance},
           {"key": "yarn.resourcemanager.address" , "value" :  "%s:8032" % master_instance},
           {"key": "yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_instance},
           {"key": "yarn.timeline-service.hostname" , "value" :  "%s" % master_instance},
           {"key": "yarn.web-proxy.address", "value" : "%s:20888" % master_instance},
           {"key": "mapreduce.jobhistory.address", "value" : "%s:10020" % master_instance},
           # Required for EMRFS
           {"key": "yarn.resourcemanager.hostname" , "value" :  "%s" % master_instance}
        ]
    }
    hive_keys = {
        "enabled": True,
        "hiveServer2Host" : master_instance,
        "executionConfigsGenericOverrides" : [
           {"key": "fs.defaultFS", "value" : "hdfs://%s:8020" % master_instance},
           {"key": "yarn.resourcemanager.address" , "value" :  "%s:8032" % master_instance},
           {"key": "yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_instance},
           {"key": "yarn.timeline-service.hostname" , "value" :  "%s" % master_instance},
           {"key": "tez.tez-ui.history-url.base", "value": "http://%s:8080/tez-ui/" % master_instance}
           # Do not define hive.metastore.uris which confuses hproxy
           # This disables the use of "Hive CLI global metastore" engine
           # Same for hive.metastore.client.factory.class
        ]
    }
    impala_keys = {
        "enabled": False
    }
    spark_keys = {
        "sparkEnabled":  True,
        "executionConfigsGenericOverrides" : [
           {"key": "spark.hadoop.fs.defaultFS", "value" : "hdfs://%s:8020" % master_instance},
           {"key": "spark.hadoop.yarn.resourcemanager.address" , "value" :  "%s:8032" % master_instance},
           {"key": "spark.hadoop.yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_instance},
           {"key": "spark.hadoop.hive.metastore.uris" , "value" :  "thrift://%s:9083" % master_instance},
           {"key": "spark.hadoop.yarn.web-proxy.address", "value": "%s:20888" % master_instance},
           {"key": "spark.yarn.historyServer.address", "value": "%s:18080" % master_instance},
           {"key": "spark.eventLog.dir" , "value" :  "hdfs:///var/log/spark/apps"}
        ]
    }
    if metastoreClientFactoryClass:
        spark_keys["executionConfigsGenericOverrides"].append(
                {"key": "spark.hadoop.hive.metastore.client.factory.class", "value": metastoreClientFactoryClass}
            )

    if create_user_dir:
        username = pwd.getpwuid(os.geteuid()).pw_name
        homedir = "hdfs://%s:8020/user/%s" % (master_instance, username)
        logging.info("creating home directory %s" % homedir)
        env = copy.deepcopy(os.environ)
        # Group 'hadoop' is in dfs.permissions.superusergroup on EMR
        env["HADOOP_USER_NAME"] = "hadoop"
        subprocess.check_call(["hdfs", "dfs", "-mkdir", "-p", homedir], env=env)
        subprocess.check_call(["hdfs", "dfs", "-chown", username, homedir], env=env)

    if create_databases:
        dbs = [ db.strip() for db in create_databases.split(',') if db.strip() ]
        if dbs:
            logging.info("creating hive databases %s" % dbs)
            subprocess.check_call(["beeline", "-u", "jdbc:hive2://%s:10000" % master_instance, "-e",
                    ' '.join([ 'create database if not exists `%s`;' % db for db in dbs ])
                ])
                
    logging.info("done attaching cluster")

    return [{'hadoop':hadoop_keys, 'hive':hive_keys, 'impala':impala_keys, 'spark':spark_keys}, {
        "emrClusterId":  cluster_id
    }]


def get_current_region():
    """Returns the AWS region of the calling process, if available, else None"""

    try:
        return requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document").json()['region']
    except:
        logging.error("could not retrieve current AWS region")
        traceback.print_exc()
        return None


def get_current_subnet():
    """Returns the EC2 subnet of the calling process, if available, else None"""

    try:
        mac = requests.get("http://169.254.169.254/latest/meta-data/mac").text
        return requests.get("http://169.254.169.254/latest/meta-data/network/interfaces/macs/%s/subnet-id" % mac).text
    except:
        logging.error("could not retrieve current EC2 subnet")
        traceback.print_exc()
        return None


def get_emr_client(config, region):
    """Returns a boto3 EMR client object"""

    role = config.get("assumeRole")
    access_key = config.get("accessKey")
    secret_key = config.get("secretKey")

    if role:
        sts = boto3.client("sts")
        try:
            response = sts.assume_role(RoleArn=role, RoleSessionName="dss-emr-access")
        except:
            logging.error("could not assume role %s" % role)
            traceback.print_exc()
            raise
            
        access_key = response["Credentials"]["AccessKeyId"]
        secret_key = response["Credentials"]["SecretAccessKey"]
        session_token = response["Credentials"]["SessionToken"]
        
        return boto3.client("emr", region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)
    elif access_key and secret_key:
        return boto3.client("emr", region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    else:
        return boto3.client('emr', region_name=region)
