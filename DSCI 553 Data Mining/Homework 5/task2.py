from blackbox import BlackBox
import sys
import time
import binascii
from statistics import mean, median

start_time = time.time()

args = sys.argv[:]

input_file = args[1]
stream_size = int(args[2])
num_asks = int(args[3])
output_file = args[4]

#input_file = 'users.txt'
#stream_size = 300
#num_asks = 30
#output_file = 'output2.csv'

actual_num = 0
estimated_num = 0

hash_values_num = 16
# a = [118, 557, 672, 982, 457, 211, 290, 491, 213, 562, 788, 672, 391, 102, 197, 302]
# b = [691, 295, 306, 673, 692, 204, 901, 572, 882, 139, 801, 792, 598, 756, 234, 126]
# p = [4057, 2393, 2411, 2143, 2957, 2333, 3701, 4211, 5399, 5557, 2791, 6553, 1663, 1523, 1123, 7057]
hash_function_list = [[118,691,4057],[557,295,2393],[672,306,241],[982,673,2143],[457,692,2957],[211,204,2333]\
                    ,[290,901,3701],[491,572,4211],[213,882,5399],[562,139,5557],[788,801,2791],[672,792,6553]\
                    ,[391,598,1663],[102,756,1523],[197,234,1123],[302,126,7057]]

def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')), 16)
    m = 2 ** hash_values_num

    for f in hash_function_list:
        result.append(((f[0] * user_int + f[1]) % f[2]) % m)
    return result

def estimate_count(est):
    avg_lst = []
    j = 0
    for i in range(4, hash_values_num):
        temp = est[j:i]
        avg_lst.append(mean(temp))
        j = i
        i += 4
    return median(avg_lst)

def flajolet_martin(users, time):
    global actual_num, estimated_num
    estimations = []
    hash_values = []

    for user_id in users:
        hash_value = myhashs(user_id)
        hash_values.append(hash_value)

    for i in range(hash_values_num):
        max_tralzeroes = -1
        for hash_value in hash_values:
            binary = bin(hash_value[i])[2:]
            trailzeros = len(binary) - len(binary.rstrip('0'))
            if trailzeros > max_tralzeroes:
                max_tralzeroes = trailzeros
        estimations.append(2 ** max_tralzeroes)

    estimated_count = int(estimate_count(estimations))
    estimated_num += estimated_count
    actual_num += len(set(stream_users))
    f.write(str(time) + ',' + str(len(set(stream_users))) + ',' + str(estimated_count) + '\n')

f = open(output_file, 'w')
f.write('Time,Ground Truth,Estimation\n')

bx = BlackBox()
for ask in range(num_asks):
    stream_users = bx.ask(input_file, stream_size)
    flajolet_martin(stream_users, ask)
f.close()
print('Final Result:',float(estimated_num / actual_num))
print('Duration:', time.time() - start_time)
