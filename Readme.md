
La pipeline d'extraction est pensé pour être schéduleée et exécuter dans airflow et contien deux grandes étapes qui représenteront chacune une task du dag ETL
 - La partie extraction : un opérateur Python qui se charge d'éxécuter le script python d'extraction des données et de les charger dans le bucket "rawzone" dans MinIO"

 - La partie clean_and_transform : un opérateur Bash qui se charge de soumettre via Spark-Submit la tâche de cleaning et de transformation des données à un clusteur spark composé d'un master et deux worker. A l'issu de ce traitement la donnée est chargée dans le bucket "gold" toujours dans MinIO.

## NB : 
La configuration Airflow n'étant pas encore terminée, le projet ne peut s'éxecuter de bout en bout de manière schédulée et automatisée. Néanmoins les deux grandes paties sont testables (exécuter à la main les différents scripts à la main) de manière séquentielles afin de simuler l'extraction puis la nettoyage et la transformation, avec chargement de la données à la fin de chaque étape

# Présenation de l'architecture du projet

# Comment executer le projet ?
Nous avons fait le choix de dockeriser tous les services nécéssaires au fonctionnement de l'application 