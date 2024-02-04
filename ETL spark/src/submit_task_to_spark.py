import subprocess
from Extract.utils import get_container_ip

def lancer_commande_docker(master_spark_IP):
    """Fonction qui demarre un subprcess afin d'executer la commande de soumission d'une tâche au master spark 

    Args:
        master_spark_IP (_type_): l'IP du master spark
    """
    commande = [
        "docker",
        "exec",
        "etlspark-spark-master-1",
        "spark-submit",
        "--master",
        "spark://"+master_spark_IP+":7077",
        "/spark/data/Transform/clean_and_transform.py"
    ]

    try:
        subprocess.run(commande, check=True)
        print("La commande a été exécutée avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de la commande : {e}")

master_spark_IP = get_container_ip("etlspark-spark-master-1", "spark_minio")
lancer_commande_docker(master_spark_IP)
