import os
import sys
import yaml

rootdir = os.path.join(os.environ["HOME"], "ycsbtest")
workloaddir = os.path.join(rootdir, "workloads")
resultdir = os.path.join(rootdir, "results")

if __name__ == '__main__':
    with open("node.yaml", "r") as file:
        nodes = yaml.load(file)
    master = nodes['master']
    slaves = nodes['slave']
    num_slaves = len(slaves)

    if (len(sys.argv) == 1):
        print(f"usage: {sys.argv[0]} command")
    if (sys.argv[1] == "deploy"):
        with open("/root/hdfs/config/hosts", "w") as file:
            file.write('''
                127.0.0.1       localhost
                ::1     localhost ip6-localhost ip6-loopback
                fe00::0 ip6-localnet
                ff00::0 ip6-mcastprefix
                ff02::1 ip6-allnodes
                ff02::2 ip6-allrouters
            ''')
            file.write(f"{master} master\n")
            for i in range(num_slaves):
                file.write(f"{slaves[i]} slave{i}\n")
        with open("/root/hdfs/config/workers", "w") as file:
            for i in range(num_slaves):
                file.write(f"slave{i}\n")
        os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/hadoop/bin"
        os.system("/root/hdfs/install.sh")
        for i in range(num_slaves):
            os.system(f"scp -r /root/hdfs/config slave{i}:/root/hdfs/")
            os.system(f"ssh slave{i} /root/hdfs/install.sh")
        
        with open("/root/hbase/config/regionservers", "w") as file:
            for i in range(num_slaves):
                file.write(f"slave{i}\n")
        os.system("/root/hbase/install.sh")     
    elif (sys.argv[1] == "start"):
        os.system("/root/hdfs/start-master.sh")
        os.system("/root/hbase/start-master.sh")
        # for i in range(num_slaves):
        #     os.system(f"ssh slave{i} /root/hdfs/start-dfs.sh")
    elif (sys.argv[1] == "pretest"):
        os.system("hbase shell < ./pretest.cmd")
    elif (sys.argv[1] == "test"):
        if os.path.exists(resultdir):
            os.system(f"rm -r {resultdir}")
        os.makedirs(resultdir, mode=0o755)
        mp = {}
        for _, _, files in os.walk(workloaddir):
            for workloadfile in files:
                elems = workloadfile.split('-')
                mp.setdefault(elems[0], [])
                mp[elems[0]].append(workloadfile)
        for key, files in mp.items():
            print(f"load {files[0]}")
            os.system(f"/root/ycsb-0.17.0/bin/ycsb load hbase20 -P {os.path.join(workloaddir, files[0])} -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family")
            for file in files:
                print(f"run {file}")
                os.system(f"/root/ycsb-0.17.0/bin/ycsb run hbase20 -P {os.path.join(workloaddir, file)} -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family > {os.path.join(resultdir, file)} 2> {os.path.join(resultdir, file)}")
    elif (sys.argv[1] == "stop"):
        os.system("stop-hbase.sh")
        os.system("stop-dfs.sh")
        os.system("./clean.sh")
        # for i in range(num_slaves):
        #     os.system(f"ssh slave{i} /root/hdfs/stop-all.sh")
    elif (sys.argv[1] == "generate"):
        os.system("python3 generator.py")
    elif (sys.argv[1] == "test"):
        os.system("")
    else:
        print(f"unknown command {sys.argv[1]}")