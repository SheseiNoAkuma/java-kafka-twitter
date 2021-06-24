# TODO 

[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) is how install elastic with docker

1. `docker pull docker.elastic.co/elasticsearch/elasticsearch:7.13.2`
2. `docker run -p 9200:9200 -p 9300:9300 -d --name elastic -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.13.2`
