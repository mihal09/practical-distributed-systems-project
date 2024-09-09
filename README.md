# practical-distributed-systems-project

## Deployment

1.Build docker images
```sh
 bash build-aggregate-processor.sh
 bash build-app-server.sh 
```

2. Add labels for specific machines:
```sh
sudo docker node update --label-add '{service_name}=true' stxxvm1xx
```

We used:
- haproxy: 101
- front: 101-13
- aerospike: 103-107
- kafka-1: 108
- kafka-2: 109
- processor: 110

3. Add persistent storage directories:
```sh
for i in {3..7}; do sshpass -p "hwmzg9zt" ssh -o StrictHostKeyChecking=no "st108vm10${i}.rtb-lab.pl" "mkdir -p /home/st108/aerospike_data"; done
```

4. Deploy using docker stack:
```sh
sudo docker stack deploy -c deployment/docker-compose.yml test
```