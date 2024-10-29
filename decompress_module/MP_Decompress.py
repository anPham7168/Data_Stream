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
            # Read each row and convert values to the appropriate type
            quantitize = float(row[0])
            lower_slp = float(row[1])
            upper_slp = float(row[2])
            time =  ast.literal_eval(row[3]) # assuming time is a single integer

            # Calculate the average slope
            average_slope = (lower_slp + upper_slp) / 2

            # Append tuple to Groups_b
            Groups_b.append((quantitize, average_slope, time))

    return Groups_b
def read_groups(file_path):
    Groups = []
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            lower = float(row[0])
            upper = float(row[1])
            time =ast.literal_eval(row[2])
            average_slope = round((lower + upper) / 2,2)
            Groups.append((None,average_slope,time))
    return Groups

Groups_b = read_groups_b('../result/mix-piece/MP_Compress_Groups_b.csv')
Groups = read_groups('../result/mix-piece/MP_Compress_Groups.csv')
Rest = read_groups('../result/mix-piece/MP_Compress_Rest.csv')

list_tuple=[]

for interval in Groups_b:
    for t in  interval[2]:
        list_tuple.append((interval[0],interval[1],t))
for interval in Groups:
    for i in range (0,len(interval[2])):
        list_tuple.append((interval[0],interval[1],interval[2][i]))
for interval in Rest:
    for i in range (0,len(interval[2])):
        list_tuple.append((interval[0],interval[1],interval[2][i]))

list_tuple.sort(key=lambda x: x[2])

