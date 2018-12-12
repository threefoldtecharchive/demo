# The demo repo is using some scripts to simplify the installation of s3 and give  you some handy tools to play with it.


## Quick Start
1- Create a `demo.yaml` file as follows:
```
zerotier:
  id: d3ecf5726df094cd
  token: zt-token

robot: # detail about the local robot
  url: 'http://localhost:6600'
  ```
2- Run `./demo.py --config demo.yaml`
Once you ran this command, you will be inside a python shell and you can use the demo object to create a s3 and play with it

3- To install s3
   - use `demo.deploy_n(n, farm, size=20000, data=4, parity=2, login='admin', password='adminadmin')`
     where n is the number of  s3 instances you need to deploy
     
