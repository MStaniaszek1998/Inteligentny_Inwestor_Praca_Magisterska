#!/bin/bash

delete_volume() {
  volume_name=$1
  environment=$2
  sudo rm -r "/var/lib/docker/volumes/${volume_name}/_data"
  #sudo rm -rf "${environment}/${volume_name}"
  docker volume rm "$volume_name"
}

check_if_volume_exists() {
  volume_name=$1
  environment=$2
  x=$(docker volume ls -q | grep -w "$volume_name")
  if [[ "$x" == "$volume_name" ]]; then
    echo "Removing [$volume_name]"
    delete_volume "$volume_name" "$environment"
  fi
}

create_volume() {
  volume_name="$2_$1"
  echo "$volume_name"
  volume_name=${volume_name:l}
  echo "$volume_name"
  environment=$2
  check_if_volume_exists "$volume_name" "$environment"
  if [[ "$environment" == "DEVELOP" ]]; then
    environment=$DEVELOP
  elif [[ "$environment" == "PREPROD" ]]; then
    environment=$PREPROD
  elif [[ "$environment" == "PROD" ]]; then
    environment=$PROD
  fi
  echo environment
  docker volume create "$volume_name"
  sudo rm -r "/var/lib/docker/volumes/${volume_name}/_data"

  
   mkdir -p "${environment}/${volume_name}"
   sudo ln -s "${environment}/${volume_name}" "/var/lib/docker/volumes/${volume_name}/_data"
}

#create_volume postgres DEVELOP true

<<<<<<< HEAD
create_volume data_collector PREPROD false
#create_volume postgres PREPROD false

#create_volume data_collector PROD false
#create_volume postgres PROD true
=======
#create_volume data_collector PREPROD false
#create_volume postgres PREPROD false

create_volume data_collector PROD false
create_volume postgres PROD true
>>>>>>> deploy-docker-on-preprod
