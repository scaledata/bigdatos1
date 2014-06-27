1. Manually rollover an edit log
#hdfs dfsadmin -rollEdits

2. Configure 2 edit log directories
#pwd
/usr/local/hadoop/etc/hadoop
#vim hdfs-site.xml

3. Edit Log parser
#hdfs oev -i edits -o edits.xml

4. Changes to fuse.py
#pwd
/home/dev1/workspace/BigDatos/third-party/fuse/fuse/fusepy
#sudo cp fuse.py /usr/local/lib/python2.7/dist-packages/fuse.py

