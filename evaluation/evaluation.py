import csv


def load_data(file_path):
    execution_time=[]
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            phase1 = float(row[0])
            phase2 = float(row[1])
            execution_time.append((phase1, phase2))
    return execution_time

exec_time_phase1_SP ,exec_time_phase2_SP = load_data('./execution_time_SP.csv')[0]
exec_time_phase1_MP ,exec_time_phase2_MP = load_data('./execution_time_MP.csv')[0]

phase1 = round(exec_time_phase2_MP/exec_time_phase2_SP,2)
phase2 = round(exec_time_phase1_MP/exec_time_phase1_SP,2)
print(f'-___________________________________________________________')
print(f'|         |Execution time phase 1 | Execution time phase 2  |')
print(f'|---------|-----------------------|-------------------------|')
print(f'|Sim Piece| {exec_time_phase1_SP} | {exec_time_phase2_SP}  |')
print(f'|---------|-----------------------|-------------------------|')
print(f'|Mix Piece| {exec_time_phase1_MP} | {exec_time_phase2_MP} |')
print(f'|___________________________________________________________|')

if phase1 > 0:
    print(f'Sim Piece algorithm execute each data point in phase 1 faster than Mix Piece algorithm {phase1} times')
else :
    print(f'Sim Piece algorithm execute each data point in phase 1 lower than Mix Piece algorithm {phase1} times')

if phase2 > 0:
    print(f'Sim Piece algorithm execute each buffer with 100 data points in phase 2 faster than Mix Piece algorithm {phase2} times')
else :
    print(f'Sim Piece algorithm execute each buffer with 100 data points in phase 2 lower than Mix Piece algorithm {phase2} times')