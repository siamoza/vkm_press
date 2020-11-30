import random
import time

x = 50
y = 50
radius = 10
for z in range(1000):
    a = random.randint(10, 90)
    b = random.randint(10, 90)
    if (a - radius) < x < (a + radius) and (b - radius) < y < (b + radius):
        print('Пара ', a, b, 'БЛИЗКО ------ !!! ------')
    else:
        print('Пара ', a, b, 'подходит')
    time.sleep(0.3)
