import pandas as pd
import os
import random


def func1(t):
    return round((-92000 / (518400 * 518400)) * t**2 + 92000, 2)

def func2(t):
    return round(3.2 * t - 1658880, 2)

def func3(t):
    return round(92000, 2)

def func4(t):
    return round((-1.027e-7) * t**2 + 129567, 2)

def func5(t):
    return round(3.2 * t - 3594240, 2)

def parabolic(a, b, c, t):
    return round(a*t*t +b *t +c,2)

data = []
for i in range (0,200):
    tup = (i, parabolic(-0.01,2,0,i)+random.randint(-5,5))
    data.append(tup)

file_path = "./streaming_data.csv"

# Save to CSV, with headers only if the file does not exist
df = pd.DataFrame(data, columns=['Timestamp', 'Value'])
if not os.path.isfile(file_path):
    df.to_csv(file_path, mode='w', index=False, header=True)
else:
    df.to_csv(file_path, mode='a', index=False, header=False)
