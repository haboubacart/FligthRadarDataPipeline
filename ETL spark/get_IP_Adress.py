import docker

def get_container_ip(container_name, network):
    """_summary_

    Args:
        container_name (_type_): _description_
        network (_type_): _description_

    Returns:
        _type_: _description_
    """
    client = docker.from_env()
    container_attr = client.containers.get(container_name).attrs
    dict_cont = dict(container_attr['NetworkSettings']['Networks'])
    return (dict_cont["etlspark_"+network]['IPAddress'])

print(get_container_ip("etlspark-spark-master-1", "spark_minio"))