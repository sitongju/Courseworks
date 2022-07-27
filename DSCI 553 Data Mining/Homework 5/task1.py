from blackbox import BlackBox
import binascii
import sys
import time

start_time = time.time()

args = sys.argv[:]

input_file = args[1]
stream_size = int(args[2])
num_asks = int(args[3])
output_file = args[4]

#input_file = 'users.txt'
#stream_size = 100
#num_asks = 30
#output_file = 'output1.csv'

filter_bit_array = [0] * 69997
user_set = set()

#a = [1,10,11,47,89]
#b = [6,43,56,71,97]
hash_function_list = [[10,6],[13,43],[22,56],[89,71],[105,97]]

def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')), 16)

    for f in hash_function_list:
        result.append((f[0] * user_int + f[1]) % 69997)
    return result

def bloom_filter(users_stream, time):
    global user_set
    global filter_bit_array

    fp_num = 0 # false positive
    tn_num = 0 # true negative

    for user_id in users_stream:
        num_1s = 0
        hash_values = myhashs(user_id)
        for value in hash_values:
            if filter_bit_array[value] == 1:
                num_1s += 1
            else:
                filter_bit_array[value] = 1

        if user_id not in user_set:
            if num_1s == len(hash_values):
                # FP: user identified in S but not actually in S
                fp_num += 1
            else:
                # TN: user not identified in S but actually in S
                tn_num += 1

        user_set.add(user_id)
    if fp_num == 0 and tn_num == 0:
        fpr = 0.0
    else:
        fpr = float(fp_num / float(fp_num + tn_num))
    f.write(str(time) + ',' + str(fpr) + '\n')


f = open(output_file, 'w')
f.write('Time,FPR\n')
bx = BlackBox()
for ask in range(num_asks):
    stream_users = bx.ask(input_file, stream_size)
    bloom_filter(stream_users, ask)
f.close()
print('Duration:', time.time() - start_time)


