1. Configure 2 edit log directories
#pwd
/usr/local/hadoop/etc/hadoop
#vim hdfs-site.xml
Add the following line: 

<property>
   <name>dfs.name.edits.dir</name>
   <value>file:/home/dev1/hadoop_play/hadoop_store/hdfs/namenode,file:/home/dev1/hadoop_play/hadoop_store/hdfs/namenode/edit2</value>
 </property>


2. Changes to fuse.py
#pwd
/home/dev1/workspace/BigDatos/third-party/fuse/fuse/fusepy
#sudo cp fuse.py /usr/local/lib/python2.7/dist-packages/fuse.py

3. Install Binary data parser
#sudo pip install construct

4. Run App Listener alone
#PYTHONPATH=../../include python thread_main.py 10.0.0.3 /home/dev1/hadoop_play/hadoop_store/hdfs/namenode/edit2/current
*********************************************
!!!These are not used now, do not proceed!!!
*********************************************
1. Edit Log parser
#hdfs oev -i edits -o edits.xml

2. Manually rollover an edit log
#hdfs dfsadmin -rollEdits
