import os

import pandas as pd
import matplotlib.pyplot as plt
import ast
import csv

def read_groups_b(file_path):
    Groups_b = []
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row if present
        for row in csv_reader:
            quantitize = float(row[0])
            slope = float(row[1])
            time = ast.literal_eval(row[2])
            # Append tuple to Groups_b
            Groups_b.append((quantitize, slope, time))

    return Groups_b

def read_groups(file_path):
    Groups = []
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            slope = float(row[0])
            list_start_point = ast.literal_eval(row[1])  # (y0,x0)
            Groups.append((slope, list_start_point))
    return Groups


groups_b_path = '../result/mix-piece/MP_Compress_Groups_b.csv'
groups_path = '../result/mix-piece/MP_Compress_Groups.csv'
rest_path = '../result/mix-piece/MP_Compress_Rest.csv'

groups_b = read_groups_b(groups_b_path)
groups = read_groups(groups_path)
rest= read_groups(rest_path)

list_segment = []  #(start_time, y at start time , slope )

for interval in groups_b:
    for t in interval[2]:
        tmp_segment = (t, interval[0], interval[1])
        list_segment.append(tmp_segment)

for interval in groups:
    for s in interval[1]:
        tmp_segment = (s[1], s[0], interval[0])
        list_segment.append(tmp_segment)

for interval in rest:   #interval = (slope, [(y,x)])
    for s in interval[1]:  #s = (y,x)
        tmp_segment = (s[1], s[0], interval[0]) #(start time , y at start time  , slope)
        list_segment.append(tmp_segment)

list_segment.sort(key=lambda x: x[0])

data_points = [] #(x,y)
for i in range (0,len(list_segment)):
    x0=list_segment[i][0]
    y0=list_segment[i][1]
    slope=list_segment[i][2]
    b= y0-x0*slope
    if i == len(list_segment)-1:
        end_time = x0+10 # assump that the last segment continue in 10 time points
    else :
        end_time = list_segment[i+1][0]
    for t in range (x0,end_time):
        y=round(slope*t + b,2)
        data_points.append((t,y))

output_file_path ='../result/mix-piece/MP_Decompress.csv'

df = pd.DataFrame(data_points, columns=['Timestamp','Value'])
if not os.path.isfile(output_file_path):
    df.to_csv(output_file_path, mode='w', index=False, header=True)
else:
    df.to_csv(output_file_path, mode='a', index=False, header=False)