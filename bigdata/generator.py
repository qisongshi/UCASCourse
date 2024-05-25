import os
import sys

def workloada(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=0.5\n")
        file.write(f"updateproportion=0.5\n")
        file.write(f"scanproportion=0\n")
        file.write(f"insertproportion=0\n")
        file.write("\n")
        file.write("requestdistribution=zipfian\n")
        file.write(f"zipfconstant={zipfconstant}\n")

def workloadb(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=0.95\n")
        file.write(f"updateproportion=0.05\n")
        file.write(f"scanproportion=0\n")
        file.write(f"insertproportion=0\n")
        file.write("\n")
        file.write("requestdistribution=zipfian\n")
        file.write(f"zipfconstant={zipfconstant}\n")

def workloadc(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=1\n")
        file.write(f"updateproportion=0\n")
        file.write(f"scanproportion=0\n")
        file.write(f"insertproportion=0\n")
        file.write("\n")
        file.write("requestdistribution=zipfian\n")
        file.write(f"zipfconstant={zipfconstant}\n")

def workloadd(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=0.95\n")
        file.write(f"updateproportion=0\n")
        file.write(f"scanproportion=0\n")
        file.write(f"insertproportion=0.05\n")
        file.write("\n")
        file.write("requestdistribution=latest\n")
    
def workloade(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=0\n")
        file.write(f"updateproportion=0\n")
        file.write(f"scanproportion=0.95\n")
        file.write(f"insertproportion=0.05\n")
        file.write("\n")
        file.write("requestdistribution=zipfian\n")
        file.write(f"zipfconstant={zipfconstant}\n")
        file.write("\n")
        file.write("maxscanlength=100\n")
        file.write("\n")
        file.write("scanlengthdistribution=uniform\n")
    
def workloadf(
    filepath,
    zipfconstant,
    recordsize,
    operationsize,
):
    with open(filepath, "w") as file:
        file.write(f"recordcount={recordsize//1000}\n")
        file.write(f"operationcount={operationsize}\n")
        file.write(f"workload=site.ycsb.workloads.CoreWorkload\n")
        file.write("\n")
        file.write(f"readallfields=true\n")
        file.write("\n")
        file.write(f"readproportion=0.5\n")
        file.write(f"updateproportion=0\n")
        file.write(f"scanproportion=0\n")
        file.write(f"insertproportion=0.5\n")
        file.write("\n")
        file.write("requestdistribution=zipfian\n")
        file.write(f"zipfconstant={zipfconstant}\n")

workloads = [workloada, workloadb, workloadc, workloadd, workloade, workloadf]

recordsizes = [2, 4, 8, 16]
operationscales = [1.5]
zipfconstants = [0, 0.2, 0.4, 0.6, 0.8, 0.99]

# rootdir = os.path.join("ycsbtest")
rootdir = os.path.join(os.environ["HOME"], "ycsbtest")
workloaddir = os.path.join(rootdir, "dataset")
os.makedirs(workloaddir, mode=0o755, exist_ok=True)
for workload in workloads:
    for recordsize in recordsizes:
        for operationscale in operationscales:
            for zipfconstant in zipfconstants:
                filename = "-".join([str(recordsize), str(operationscale), str(zipfconstant), workload.__name__])
                __recordsize = recordsize * 1024 * 1024 * 1024
                operationsize = int(__recordsize * operationscale)
                workload(os.path.join(workloaddir, filename), zipfconstant, __recordsize, operationsize)