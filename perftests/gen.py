import random
import os
import argparse
import sys
import subprocess
import json
import time

parser = argparse.ArgumentParser("performance test")
parser.add_argument("outdir", type=str, help="output dir")
parser.add_argument("--seed", type=int, help="random generator seed",
                    default=time.gmtime(0), dest="seed")
parser.add_argument("--rfile", type=str, help="output r csv file",
                    default="r.csv", dest="rfile")
parser.add_argument("--sfile", type=str, help="output s csv file",
                    default="s.csv", dest="sfile")
parser.add_argument("--numr", type=int, help="num rows for r",
                    default=10000, dest="numr")
parser.add_argument("--nums", type=int, help="num rows for s",
                    default=10000, dest="nums")
parser.add_argument("--amax", type=int, help="max diff a values",
                    default=1000, dest="amax")
parser.add_argument("--selrater", type=float, help="selectivity rate r",
                    default=0.75, dest="selrater")
parser.add_argument("--selrates", type=float, help="selectivity rate s",
                    default=0.75, dest="selrates")
args = vars(parser.parse_args())

NUMROWS_R = args["numr"]
NUMROWS_S = args["nums"]

A_MIN = 0
A_MAX = args["amax"]

SELECTIVITY_RC = args["selrater"]
RC_MIN = 1
RC_MAX = 9

SELECTIVITY_SF = args["selrates"]
SF_MIN = 31
SF_MAX = 99

RD_MIN = -(2 ** 30)
SG_MAX = 2 ** 30

TEMPLATE_SQL = "templates/perftest.sql.template"
TEMPLATE_165 = "templates/perftest.txt.template"

OUTDIR = args["outdir"]
RFILE = "%s/%s" % (OUTDIR, args["rfile"])
SFILE = "%s/%s" % (OUTDIR, args["sfile"])
FILE_SQL = "%s/%s" % (OUTDIR, "perftest.sql")

JOINTYPES = ["hash", "sort", "loop", "tree"]

RFILE_BLANK = "{{ RFILE }}"
SFILE_BLANK = "{{ SFILE }}"
JOINTYPE_BLANK = "{{ JOINTYPE }}"

SETTINGS_FILE = "%s/settings.json" % OUTDIR

SEED = args["seed"]

# init our random generator
random.seed(SEED)

# try to make the directory
os.makedirs(OUTDIR)

# create settings file
settingsf = open(SETTINGS_FILE, "w")
settingsf.write(json.dumps(args, indent=2))
settingsf.close()

# create templates
templatef = TEMPLATE_SQL
realf = FILE_SQL
template = open(templatef, "r")
real = open(realf, "w")
for l in template.readlines():
    l = l.replace(RFILE_BLANK, args["rfile"])
    l = l.replace(SFILE_BLANK, args["sfile"])
    real.write(l)
template.close()
real.close()

for jointype in JOINTYPES:
    if jointype == "tree" and SELECTIVITY_SF != 1.0:
        print "must have selectivity 1.0 for tree join"
        sys.exit(1)
    templatef = TEMPLATE_165
    realf = "%s/perftest_%s.txt" % (OUTDIR, jointype)
    template = open(templatef, "r")
    real = open(realf, "w")
    for l in template.readlines():
        l = l.replace(RFILE_BLANK, args["rfile"])
        l = l.replace(SFILE_BLANK, args["sfile"])
        l = l.replace(JOINTYPE_BLANK, jointype)
        real.write(l)
    template.close()
    real.close()

# link scripts
pwd = os.getcwd()
for script in ["test_sql.sh", "test_server.sh", "test_client.sh"]:
    subprocess.call(("ln -s ../scripts/%s %s/%s" % (script, OUTDIR, script)).split())

# generate r rows
ra = [random.randint(A_MIN,A_MAX) for _ in range(NUMROWS_R)]
rc = [random.randint(RC_MIN,RC_MAX) if i < SELECTIVITY_RC * NUMROWS_R
      else RC_MAX + 1
      for i in range(NUMROWS_R)]
rd = [random.randint(RD_MIN, 0) for _ in range(NUMROWS_R)]
random.shuffle(list(ra))
random.shuffle(rc)
random.shuffle(rd)

try:
    os.remove(RFILE)
except:
    pass
fl = open(RFILE, "w")
fl.write("ra,rc,rd\n")
for (a,c,d) in zip(ra,rc,rd):
    fl.write("%d,%d,%d\n" % (a,c,d))
fl.close()

# generate s rows
sa = [random.randint(A_MIN,A_MAX) for _ in range(NUMROWS_S)]
sf = [random.randint(SF_MIN,SF_MAX) if i < SELECTIVITY_SF * NUMROWS_S
      else SF_MAX + 1
      for i in range(NUMROWS_S)]
sg = [random.randint(0,SG_MAX) for _ in range(NUMROWS_S)]
random.shuffle(sa)
random.shuffle(sf)
random.shuffle(sg)

try:
    os.remove(SFILE)
except:
    pass
fl = open(SFILE, "w")
fl.write("sa,sf,sg\n")
for (a,f,g) in zip(sa,sf,sg):
    fl.write("%d,%d,%d\n" % (a,f,g))
fl.close()
