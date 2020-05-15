## TiKV cluster

The best way to run a TiKV cluster is through binary file. Please refer to https://tikv.org/docs/3.0/tasks/deploy/binary/ to see how to deploy a TiKV cluster on different port of one machine. 

Once the deployment is done. You can pass `127.0.0.1:2379` as pd address to config and start a connection with TiKV from there.