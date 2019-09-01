

class Constant(object):
    OnDemand = "ON_DEMAND"
    Spot = "SPOT"
    Master = "MASTER"
    Core = "CORE"
    Task = "TASK"
    Gp2 = "gp2"
    AwaitingFulfillment = "AWAITING_FULFILLMENT"
    Provisioning = "PROVISIONING"
    Bootstrapping = "BOOTSTRAPPING"
    Running = "RUNNING"


class Arg(object):
    Key = "Key"
    Value = "Value"
    Name = "Name"
    Id = "Id"
    Cluster = "Cluster"
    Release = "ReleaseLabel"
    Instances = "Instances"
    Applications = "Applications"
    VisibleToAllUsers = "VisibleToAllUsers"
    JobFlowRole = "JobFlowRole"
    ServiceRole = "ServiceRole"
    Tags = "Tags"
    SecurityConfig = "SecurityConfiguration"
    EbsRootVolSize = "EbsRootVolumeSize"
    Iops = "Iops"
    Configurations = "Configurations"
    InstanceGroups = "InstanceGroups"
    InstanceGroupType = "InstanceGroupType"
    Ec2InstanceAttributes = "Ec2InstanceAttributes"
    KeepJobFlowAliveWhenNoSteps = "KeepJobFlowAliveWhenNoSteps"
    Ec2SubnetId = "Ec2SubnetId"
    Ec2KeyName = "Ec2KeyName"
    AddlMasterSecurityGroups = "AdditionalMasterSecurityGroups"
    AddlSlaveSecurityGroups = "AdditionalSlaveSecurityGroups"
    TerminationProtected = "TerminationProtected"
    InstanceRole = 'InstanceRole'
    Market = 'Market'
    BidPrice = "BidPrice"
    InstanceType = "InstanceType"
    InstanceCount = 'InstanceCount'
    RunningInstanceCount = "RunningInstanceCount"
    EbsConfig = 'EbsConfiguration'
    EbsBlockDeviceConfigs = 'EbsBlockDeviceConfigs'
    EbsVolSpec = "VolumeSpecification"
    EbsVolType = "VolumeType"
    EbsVolSizeGb = "SizeInGB"
    EbsVolsPerInstance = "VolumesPerInstance"
    EbsOptimized = "EbsOptimized"
    Classification = "Classification"
    Properties = "Properties"
    LogUri = "LogUri"
    PrivateIpAddress = "PrivateIpAddress"
    Status = "Status"
    State = "State"


class Response(object):
    JobFlowId = "JobFlowId"