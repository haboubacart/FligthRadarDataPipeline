import docker
import json
# Créez une instance du client Docker
client = docker.from_env()
container_attr = client.containers.get('etlspark-minio-1').attrs
dict_cont = dict(container_attr['NetworkSettings']['Networks'])
print(dict_cont['etlspark_default']['IPAddress'])
