# README #

This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary
This is the main repository for BigDatos project.


### How do I get set up? ###

* Summary of set up
Assuming OpenStack SWIFT is already setup (it is pre-installed in Ubuntu-Dev2, Ubuntu-Server1 and
Ubuntu-Server4). 

Other than SWIFT, RabbitMQ, FUSE and Hadoop need to be setup.

* Dependencies
Assuming OpenStack SWIFT has been installed on Ubuntu-Server1 and Ubuntu-Server4 (TBD:see xxx for detailed instructions). 
Assuming SWIFT client has been installed on Ubuntu-Dev2 (TBD:see xxx for detailed instructions).
Assuming One-node Hadoop cluster has been setup on Ubuntu-Dev2 
(see https://app.asana.com/0/search/13458464068350/13095619425289 for detailed instructions).

* Deployment instructions

0. Import pre-installed VMs (Ubuntu-Dev2, Ubuntu-Server1 and Ubuntu-Server4)
0.1 Each VM has a corresponding ova file, copy these ova files to your Macbook

0.2 Install VirtualBox if you have not done so (see https://www.virtualbox.org/wiki/Downloads, download the one
for mac, for example, 'VirtualBox 4.3.12 for OS X hosts  x86/amd64')

0.3 Import three VMs, 
0.3.1 Open VirtualBox (Command+Space, and type VirtualBox)
0.3.2 File--> Import Appliance, choose the right ova file
0.3.3 On page-2, ensure to check "re-initialize the MAC address of all network cards"
0.3.4 Follow remaining instructions to finish the importing

0.4 Users for three VMs
0.4.1 dev1(username):test4me(password) for Ubuntu-Dev2
0.4.2 dev1:test4me for Ubuntu-Server1
0.4.3 dev1:test4me for Ubuntu-Server4

0.5 Configure network
0.5.1 Configure eth0 to be external bridge network, and eth1 to be private network
Use Right-Top Network Manager to configure the network by adding network configurations, the configuration
is as below (do not configure /etc/network/interfaces directly as that would conflict with network-manager. 
Also, make sure to remove the corresponding entries for eth0&eht1, otherwise network-manager would get confused):

# The loopback network interface
auto lo
iface lo inet loopback
# The primary network interface
auto eth0
iface eth0 inet dhcp
# The primary network interface
iface eth1 inet static
address 10.0.0.xx
netmask 255.255.255.0
gateway 10.0.0.1

For Ubuntu-Dev2, its address is 10.0.0.3; for Ubuntu-Server1, its address is 10.0.0.11; for Ubuntu-Server4,
its address is 10.0.0.31

0.5.2 Restart network manager
#sudo service network-manager restart
0.5.3 If the 10.0.0.x address does not show up in 'ifconfig', do the following:
#sudo ifdown ethx
#sudo ifup ethx

0.6 Configure hostname mappings
#self
10.0.0.3        ubuntu-dev2
#controller
10.0.0.11       controller
#network
10.0.0.21       network
#compute
10.0.0.31       compute1

It is recommended to reboot the VM after configuring the hostnames

1. Setup RabbitMQ
1.1  Install RabbitMQ Server on Ubuntu
#echo "deb http://www.rabbitmq.com/debian/ testing main"  | sudo tee  /etc/apt/sources.list.d/rabbitmq.list > /dev/null
#sudo wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
#sudo apt-key add rabbitmq-signing-key-public.asc
#sudo apt-get update
#sudo apt-get install rabbitmq-server -y
#sudo service rabbitmq-server start
#sudo rabbitmq-plugins enable rabbitmq_management
#sudo service rabbitmq-server restart

1.2 Install Python binding for RabbitMQ
#sudo apt-get install python-pip git-core
#sudo pip install pika==0.9.13

1.3 Add 'guest' user and allow the user to connect remotely
1.3.1 Change the password for guest
#sudo rabbitmqctl change_password guest guest

1.3.2 Allow guest to connect remotely
A complete rabbitmq.config (/etc/rabbitmq/rabbitmq.config) which does this would look like 
(Do not forget the last period):

[{rabbit, [{loopback_users, []}]}].

1.3.3 Restart rabbitmq-server
#sudo service rabbitmq-server restart

2. Setup FUSE
2.1. Download the latest stable version 2.9.3 from http://sourceforge.net/projects/fuse/files/fuse-2.X/2.9.3/, 
fuse-2.9.3.tar.gz, 

2.2. Untar the directory
#tar -xzvf fuse-2.9.3.tar.gz
2.3. Make and install
#./configure
#make
#sudo make install 

2.4. Install Python binding
2.4.1 Clone the source git repository
#git clone https://github.com/terencehonles/fusepy.git
2.4.2 Install fusepy
#sudo python ./setup.py install

!!! Do not use sudo pip install fusepy as that gives you the old version

2.5. Install Binary data parser to parse FUSE Edit Log Entries
#sudo pip install construct

3. Configure Hadoop 
3.1 Configure Hadoop to add one more Edit Log location
#pwd
/usr/local/hadoop/etc/hadoop
#vim hdfs-site.xml
Add the following line: 

<property>
   <name>dfs.name.edits.dir</name>
   <value>file:/home/dev1/hadoop_play/hadoop_store/hdfs/namenode,file:/home/dev1/hadoop_play/hadoop_store/hdfs/namenode/edit2</value>
</property>

4. Make scp happy about directories
Hard coded, on src side, you should have /home/dev1/temp directory
On target side, you should have /home/dev1/store_temp directory



* Database configuration
None yet.
* How to run tests
1. Setup the environment
1.1 Add the following line to ~/.bashrc
export PYTHONPATH=$PYTHONPATH:/home/dev1/workspace/BigDatos/src/common
1.2 Make 10.0.0.3 and 10.0.0.11 scp from each other without password
#ssh-keygen -t rsa -P ''
#cat ~/.ssh/id_rsa.pub >>~/.ssh/authorized_keys

2. Start node_agent on Ubuntu-Dev2 (10.0.0.3)
#pwd 
/home/dev1/workspace/BigDatos/src/client/node_agent
#python node_agent.py

3. Start policy_engine on Ubuntu-Server1 (10.0.0.11)
#pwd
/home/dev1/workspace/BigDatos/src/store/policy_engine
#python main.py 10.0.0.11 10.0.0.3

4. Input the policy on Ubuntu-Dev2
#pwd 
/home/dev1/workspace/BigDatos/src/client/cli
#python cli_top.py manage /t2.txt 30 sec

5. Start HDFS on Ubuntu-Dev2
#start-dfs.sh

6. Copy from local to HDFS on Ubuntu-Dev2
#pwd
/home/dev1
#hadoop fs -copyFromLocal /t2.txt

7. Check if the file is uploaded to SWIFT on Ubuntu-Dev2
#swift list demo
It should show something like below:
$ swift list demo
t7.txt

Viola! You are done with the test

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
Maohua Lu (maohua.lu@datoscloud.io)
Neville Carvalho (neville.carvalho@datoscloud.io)

