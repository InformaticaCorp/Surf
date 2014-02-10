[![Informatica](http://www.informatica.com/Images/informatica-logo.png)](http://www.informatica.com)
Surf
=====

Connecting various data collection technologies (messaging, logging, applications, etc.) to Amazon Kinesis.

[What is Surf?](https://github.com/InformaticaCorp/Surf/wiki#wiki-what-is-surf)

[Amazon Kinesis?](https://github.com/InformaticaCorp/Surf/wiki#wiki-amazon-kinesis)

[Wiki?]( https://github.com/InformaticaCorp/Surf/wiki "or click on the github wiki link...")


License (See LICENSE file for full license)
-------------------------------------------
Copyright 2013-2014 Informatica Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Code Layout
---------------

Main source code

    surf
    common

VDS API

 	vdsapi

Example configuration file. Feel free to use `surf.properties.example` as your starting point.

```
# AWS account information
aws-access-key-id: <your-access-id>
aws-secret-key: <your-secret-key>
# Kinesis Stream Name
aws-kinesis-stream-name: <your-stream-name>
# DummySource sends one message containing the current timestamp every second
# You can use any class that implements VDSSource
vds-source-class: com.informatica.baresurf.DummySource
# The next line sets the number of threads used by the
# Kinesis Async API
kinesis-parallel-requests: 10
```

Build
-----

Full clean build of package. Please make sure to set `$JAVA_HOME` to point to JDK 1.7 as it might be required to find JDK 1.7.

    $ mvn package

Afterward, a zip for the executable(s), scripts, and libs for easy install and distribution can be found here after building.

    $ assembly/target/surf-0.1-assembly.zip

Alternatively, you can use mvn and install in your own repo.

	$ mvn install

Quick Start
-----------

- Build
- Create an [Amazon Kinesis](http://aws.amazon.com/kinesis/) Stream. Instructions can be found [here](http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html).
- Create a configuration file for the node by copying `surf.properties.example` to `assembly/target/surf-0.1-assembly/surf-0.1/conf` and editing
it to include your AWS account information and Kinesis stream name. Call this file `node1.conf`.
- Start the node, called `node1`, by executing the following.

```
$ assembly/target/surf-0.1-assembly/surf-0.1/bin/surf.sh start-node node1
```

- Profit? Or, actually, wait for some messages to be sent to Kinesis.

- When done, shut down the node by running the following.

```
$ bin/surf.sh stop-node node1
```

NOTES: 

- You will have to have an AWS account and be signed up for Kinesis. For more information, see [this](https://github.com/awslabs/amazon-kinesis-client#getting-started).
- Logs for the node can be found in `assembly/target/surf-0.1-assembly/surf-0.1/log/node1-node.log`.
