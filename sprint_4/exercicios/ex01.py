
with open('number.txt') as n:
    numbers = map(int, n.readlines())
    
num_par = filter(lambda x: x % 2 == 0, numbers)
num_sorted = sorted(num_par, reverse=True)
num_top5 = num_sorted[:5]
num_top5_sum = sum(num_top5)

print(num_top5)
print(num_top5_sum)