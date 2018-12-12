# This Capacity will give you some handy tools to use in parallel with your demo object. For instance using it, you can update your zerorobots, check zeroos version, etc..

## How to use
1- Pull the repo then run this command
`~/demo/capacity# python3 capacity.py -f kristof-farm -e  10.102.104.231 -e 10.102.113.188`
This will allow you to enter a python shell in which you can use the capacity objects where

`-e`: means you will execude certain nodes from you environments

`-f`: is the farm these scripts will run against

2- Once you are already inside the shell you, can use all these functions as seen:
```
In [1]: capacity.
      capacity.capacity                 capacity.execute_all_nodes        capacity.nodes                     
      capacity.check_zos_version        capacity.farm_name                capacity.py                        
      capacity.check_zrobot_status      capacity.get_node_ip_from_node_id capacity.reboot_nodes             
      capacity.exclude_nodes            capacity.logger                   capacity.resp                     
      ```
