from blackbox import BlackBox
import random
import sys
import time

start_time = time.time()

args = sys.argv[:]

#input_file = 'users.txt'
#stream_size = 100
#num_asks = 30
#output_file = 'output3.csv'

input_file = args[1]
stream_size = int(args[2])
num_asks = int(args[3])
output_file = args[4]

random.seed(553)
reservoir = [0] * 100
seq = 0

def reservoir_sampling(users, time):
    global reservoir, seq
    if time == 0:
        # the first ask, fill in the list directly
        for i in range(stream_size):
            reservoir[i] = users[i]
        seq += 100
    else:
        for user_id in users:
            seq += 1
            if random.random() < 100/seq:
                x = random.randint(0, 99)
                reservoir[x] = user_id
    f.write(str(seq) + ',' + str(reservoir[0]) + ',' + str(reservoir[20]) + ',' + str(reservoir[40]) + ','
            + str(reservoir[60]) + ',' + str(reservoir[80]) + '\n')

f = open(output_file, 'w')
f.write('seqnum,0_id,20_id,40_id,60_id,80_id\n')

bx = BlackBox()
for ask in range(num_asks):
    stream_users = bx.ask(input_file, stream_size)
    reservoir_sampling(stream_users, ask)
f.close()
print("Duration:", time.time() - start_time)