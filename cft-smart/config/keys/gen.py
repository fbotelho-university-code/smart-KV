import shutil
import sys

if len(sys.argv) != 3:
    print "Usage : %s  LOW_RANGE HIGH_RANGE " % (sys.argv[0])
    sys.exit(0)

for i in range (int(sys.argv[1]) , int(sys.argv[2])):
    shutil.copyfile("./base.private", "privateKey" + str(i))
    shutil.copyfile("./base.pub", "publickey" + str(i) ); 
