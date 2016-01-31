import serenity_pypeline.protos.mesos_pb2
from serenity_pypeline.protos.mesos_pb2 import ResourceUsage


def getSerializedUsage():
    usage = ResourceUsage()
    resourcesTotal = usage.total.add()
    resourcesTotal.name = "cpus"
    resourcesTotal.type = 0
    return usage.SerializeToString()