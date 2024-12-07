def pares_ate(n: int):
    for i in range(2, n + 1):
        if i % 2 == 0:
            yield i

n = int(50)

print(list(pares_ate(n)))