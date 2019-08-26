import paramiko
import dataiku
import subprocess
import boto3
import tempfile
import time
from collections import Iterable
from tabulate import tabulate


class RemoteSSHClient(object):

    @staticmethod
    def get_rsa_keyfile(path):
        if not isinstance(path, str):
            raise Exception("Parameter 'path': Expected type str. Found {}".format(type(path)))

        if "s3://" in path:
            s3_path_parts = path.split("s3://")[1].split("/")
            bucket_name = s3_path_parts[0]
            bucket_key = "/".join(s3_path_parts[1:-1]) + "/"
            file_name = s3_path_parts[-1]
            keyfile = tempfile.NamedTemporaryFile(suffix=".pem")
            keyfile.write(
                boto3.resource('s3').Object(bucket_name, bucket_key + file_name).get()['Body'].read()
            )
            keyfile.seek(0)

            return paramiko.RSAKey.from_private_key_file(keyfile.name)

        return paramiko.RSAKey.from_private_key_file(path)
    
    def __init__(self, host, username, rsa_key_or_path, autoconnect=True):
        self.host = host
        self.username = username
        self.rsa_key = rsa_key_or_path
        if not isinstance(self.rsa_key, paramiko.rsakey.RSAKey):
            self.rsa_key = RemoteSSHClient.get_rsa_keyfile(rsa_key_or_path)
        
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.is_connected = False
        if autoconnect:
            self.client.connect(hostname=self.host, username=self.username, pkey=self.rsa_key)
            self.client.invoke_shell()
            self.is_connected = True
        
    def __del__(self):
        self.client.close()
        
    def connect(self):
        self.client.connect(hostname=self.host, username=self.username, pkey=self.rsa_key)
        # self.client.invoke_shell()
        self.is_connected = True

    def close(self):
        if self.client is not None and self.is_connected:
            self.client.close()
            self.is_connected = False
            print("Closed connection to host %s." % self.host)

    def execute(self, cmds, print_summary=False, disconnect=False, use_channel=False):
        if not self.is_connected:
            raise ValueError("Cannot execute commands on host %s. You first need to open a connection." % self.host)

        formatted_cmds = RemoteSSHClient._check_and_format_commands(cmds)
        
        print("Executing the following commands: '{}'".format(list(cmds)))
        runner = None
        if use_channel:
            print("Running via channel.")
            runner = self.client.get_transport().open_session()
            
        runner = runner or self.client
        _, stdout, stderr = runner.exec_command(formatted_cmds)
        cmd_run_info = {
            'cmds': formatted_cmds,
            'stdout': stdout, 
            'stderr': stderr, 
            'use_channel': use_channel
        }
        
        if use_channel:
            runner.close()

        if print_summary:
            print("\nCommand run summary:")
            print(
                tabulate(
                    list(cmd_run_info.values()), 
                    list(cmd_run_info.keys())
                ), 
                "\n"
            )

        if disconnect:
            self.close()

        return cmd_run_info
    
    @staticmethod
    def _check_and_format_commands(cmds):
        if not isinstance(cmds, Iterable) or isinstance(cmds, str):
            raise TypeError("Parameter 'cmds': Expected type Iterable or str. Found {}.".format(type(cmds)))
        
        if isinstance(cmds, Iterable):
            for c in cmds:
                if not (isinstance(c, str)):
                    raise TypeError(
                        "Parameter 'cmds' contains invalid non-str element {} of type {}".format(c, type(c))
                    )
            
            return " && ".join(cmds)
        
        return cmds
    