## ssh to ec2  instance from terminal 
1. change directory to test_key.pem  file location
```
CD path/to/test_key.pem
```
2. Use the chmod command to set more restrictive permissions on your private key file

```
chomd 600  test_key.pem 
```
3. After changing the permissions, connect to EC2 instanceusing the ssh command:
```
ssh -i "path/to/test_key.pem" ec2-user@ec2-18-133-73-36.eu-west-2.compute.amazonaws.com
```
Note that you need to replace `path/to/` with the actual directory where your private key is located 

## Runing Hadoop Jobs in AWS Ec2 Instance 

# I - word count using python

```
1. 
```