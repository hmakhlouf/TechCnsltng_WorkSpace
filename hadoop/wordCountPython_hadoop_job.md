## ssh to ec2  instance from terminal 
1. change directory to test_key.pem  file location
```
CD path/to/test_key.pem
```
2. Use the chmod command to set more restrictive permissions on your private key file

```
chomd 600  test_key.pem 
```
3. After changing the permissions, connect to EC2 instance using the ssh command:
```
ssh -i "path/to/test_key.pem" ec2-user@ec2-18-133-73-36.eu-west-2.compute.amazonaws.com
```
Note that you need to replace `path/to/` with the actual directory where your private key is located 

## Runing Hadoop Jobs in AWS Ec2 Instance 

To run a job in Hadoop using Python, you can leverage the Hadoop Streaming API, which allows you to write Map and Reduce tasks in any language that can read from standard input and write to standard output. Here are the general steps:

### 1 - Write Mapper and Reducer Scripts:
Create your Mapper and Reducer scripts in Python. These scripts should read from standard input and write to standard output. Save them as mapper.py and reducer.py, for example:

1.  Create the Mapper Script (mapper.py): 
```
echo '
# mapper.py
# Word count
#!/usr/bin/env python

import sys

# Input comes from standard input (sys.stdin)
for line in sys.stdin:
    # Remove whitespace at the beginning and the end
    line = line.strip()

    # Split the line into words
    words = line.split()

    # Output tuples (word, 1) in tab-delimited format
    for word in words:
        print "%s\t%s" % (word, 1)
' > mapper.py
```

2. Create the Reducer Script (reducer.py):
```
echo '
# reducer.py
#  Word count
#!/usr/bin/env python

import sys

current_word = None
current_count = 0
word = None

# Input comes from standard input (sys.stdin)
for line in sys.stdin:
    # Parse the input from mapper.py
    line = line.strip()

    # Split the line into word and count, separated by tab
    word, count = line.split('\t', 1)

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # If count was not a number, ignore/discard this line
        continue

    # This IF-switch works because Hadoop sorts the output of the mapper by key (here: word)
    # before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Write result to standard output
            print "%s\t%s" % (current_word, current_count)
        current_count = count
        current_word = word

# Output the last word if needed!
if current_word == word:
    print "%s\t%s" % (current_word, current_count)
' > reducer.py

```

4. Make Scripts Executable:
Make sure your scripts are executable by running the following commands

```
chmod 777 mapper.py reducer.py
```

5.  Run Hadoop Streaming Job:
Use the hadoop command with the -mapper, -reducer, and -input flags to specify your Python scripts and input data.

```
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-mapper "/usr/bin/python mapper.py" \
-reducer "/usr/bin/python reducer.py"\
 -input /tmp/USUK30/hocine/data/data.txt \
 -output /tmp/USUK30/hocine/output1
```