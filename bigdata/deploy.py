import os
import sys
import yaml

if __name__ == '__main__':
    with open("node.yaml", "r") as file:
        nodes = yaml.load(file, Loader=yaml.FullLoader)
    master = nodes['master']
    slaves = nodes['slave']
    num_slaves = len(slaves)

    if (len(sys.argv) == 1):
        print(f"usage: {sys.argv[0]} command")
    if (sys.argv[1] == "deploy"):
        with open("config/hosts", "w") as file:
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
        os.system("./start-master.sh")
        for i in range(num_slaves):
            os.system(f"scp -r config slave{i}:/root/hdfs/")
            os.system(f"ssh slave{i} /root/hdfs/install.h")
    elif (sys.argv[1] == "start"):
        for i in range(num_slaves):
            os.system(f"ssh slave{i} /root/hdfs/start.h")
    elif (sys.argv[1] == "stop"):
        os.system("./stop-all.sh")
        for i in range(num_slaves):
            os.system(f"ssh slave{i} /root/hdfs/stop-all.sh")
    else:
        print(f"unknown command {sys.argv[1]}")
            
