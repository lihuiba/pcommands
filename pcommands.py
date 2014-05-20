#!/usr/bin/python
import threading, time, subprocess, shlex, sys, argparse

parser=argparse.ArgumentParser(description="Execute a command on many machines, simultaneously and precisely")
parser.add_argument('-l', '--list', metavar='HOSTS', default='host.list', help='a file that contains a list of hosts, with each host on a single line, and optionally followed by a command to be executed on that host')
parser.add_argument('-c', '--command', help='default command to be executed on remote hosts, if none is explicitly specified in host list. Either a command or the default command should be given for each host')
args=parser.parse_args()
# print vars(args)

cmds={}
hosts=[]
defcmd=args.command
cmds.setdefault(defcmd)

for line in open('host.list').readlines():
	line=line.strip().split(' ', 2)
	host=line[0]
	hosts.append(host)
	if len(line)>1:
		cmds[host]=line[1]
	elif defcmd==None:
		print "Neither a command nor the default command is given for host %s. Exiting.\n" % host
		sys.exit(-1)

#sys.exit()


count=0
condition=threading.Condition()
output={}

def work(host):
	global output, count
	print host+' connecting\n',
	shell=subprocess.Popen(['ssh', host], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	shell.stdin.write('echo hello!\n')
	shell.stdin.flush()
	shell.stdout.readline()	
	print host+' connected\n',

	count+=1
	condition.acquire()
	condition.wait()
	condition.release()

	print host+' start\n',
	# cmd = cmds.get(host) or defcmd
	shell.stdin.write(cmds[host] + '; exit\n')
	shell.stdin.flush()
	output[host] = shell.communicate()   # returns (stdout, stderr)
	print host+' finished\n',


count=0
threads=[]
for host in hosts:
	th=threading.Thread(target=work, args=(host, ))
	threads.append(th)
	th.start()
	time.sleep(0.1)

while count<len(hosts):
	time.sleep(1)
	print count
time.sleep(1)  #sleep 1 more second to ensure all workers are waiting
condition.acquire()
condition.notify_all()
condition.release()
print 'fire!'

for i in range(len(threads)):
	threads[i].join()

for host in hosts:
	out=output[host]
	print '============================'
	print host+"'s stdout:"
	print out[0]
	if len(out[1])>0:
		print host+"'s stderr:"
		print out[1]


