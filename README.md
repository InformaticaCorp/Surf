[![Informatica](http://www.informatica.com/Images/informatica-logo.png)](http://www.informatica.com)
Surf
=====

Surf is an application that makes using Amazon Kinesis extremely simple. Surf provides simplified
mechanisms for putting data into Kinesis, and also to consume data from Kinesis and perform
meaningful operations on them. The idea is to abstract away the actual Kinesis code, so that
developers can focus on the parts that matter to them. Feel free to fork the code and play with it.

[Surf Web Page?](http://www.projectsurf.net)

[What is Surf?](https://github.com/InformaticaCorp/Surf/wiki#wiki-what-is-surf)

[Amazon Kinesis?](https://github.com/InformaticaCorp/Surf/wiki#wiki-amazon-kinesis)

[Wiki?]( https://github.com/InformaticaCorp/Surf/wiki "or click on the github wiki link...")



Build
-----
You'll need the following before you can build Surf:
* Git to clone this repository
* A recent version of the Java Development Kit (JDK). Surf has been tested with JDK 1.7.
* Apache Maven 3

Once you have the prerequisites, you can follow these steps:

```
git clone https://github.com/InformaticaCorp/Surf.git
cd Surf
mvn package
```

The build results are packaged in this file:

    $ assembly/target/surf-1.0-dist.zip

You can unzip this file in any location of your choice and run Surf from there. For convenience, the contents of this zip file
are also available here:

    $ assembly/target/surf-1.0-dist/surf-1.0

Quick Start
-----------

- Build
- Create an [Amazon Kinesis](http://aws.amazon.com/kinesis/) Stream. Instructions can be found [here](http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html).
- Create a configuration file for the node by copying `ingest.yaml` to `assembly/target/surf-0.1-assembly/surf-0.1/conf` and editing
it to include your AWS account information and Kinesis stream name. Call this file `node1.yaml`.
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
- Amazon Kinesis is a paid service. You will need to pay Amazon according to your usage.
- Logs for the node can be found in `assembly/target/surf-0.1-assembly/surf-0.1/log/node1-node.log`.


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

