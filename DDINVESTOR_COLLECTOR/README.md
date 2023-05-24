# Required components: 
- Docker 2.4 and Docker compose 
- Ubuntu 18.04 

# How to run the scrapers
export DOWNLOADED_DATA='Your folder or use ready structure'
cd ./docker 
docker-compose build -f ./preprod_scenario_one.sh
docker-compose build -f ./preprod_scenario_two.sh
docker-compose build -f ./preprod_scenario_three.sh
docker-compose run -f ./preprod_scenario_one.sh
docker-compose run -f ./preprod_scenario_two.sh
docker-compose run -f ./preprod_scenario_three.sh


