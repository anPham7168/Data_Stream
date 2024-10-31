import csv
import math
import os


import pandas as pd


def load_data(file_path):
    data_signal = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            timestamp = int(row[0])
            value = float(row[1])
            data_signal.append((timestamp, value))
    return data_signal

def save_groups_b_to_csv(groups, output_file_path):
    df = pd.DataFrame(groups, columns=['Quantized Value', 'Slope', 'Time Points'])
    if not os.path.isfile(output_file_path):
        df.to_csv(output_file_path, mode='w', index=False, header=True)
    else:
        df.to_csv(output_file_path, mode='a', index=False, header=False)

def save_groups_to_csv(groups, output_file_path):
    df =pd.DataFrame   (groups, columns=[ 'Slope', 'Array of (y0,x0)'])
    if not os.path.isfile(output_file_path):
        df.to_csv(output_file_path, mode='w', index=False, header=True)
    else:
        df.to_csv(output_file_path, mode='a', index=False, header=False)

def save_decompress_data_to_csv(decompress_data, output_file_path):
    df = pd.DataFrame (decompress_data, columns=['Timestamp', 'Value'])
    if not os.path.isfile(output_file_path):
        df.to_csv(output_file_path, mode='w', index=False, header=True)
    else:
        df.to_csv(output_file_path, mode='a', index=False, header=False)


class TimeSeriesCompressionMixPiece:
    def __init__(self,epsilon):
        self.epsilon = epsilon # temp dictionary use for storing result of phase 1
        self.b_intervals = {}
        self.tmp_b_intervals = {}
        self.buffer = 0
        self.buffer_limit=1000
        self.floor = True
        self.ceil= True
        self.b1 = None
        self.b2 = None
        self.slp_upper_1=float('inf')
        self.slp_upper_2=float('inf')
        self.slp_lower_1=float('-inf')
        self.slp_lower_2=float('-inf')
        self.length =0

    def mix_piece_phase1(self,data_point):
        Timestamp , Value = data_point # the first point of data stream
        self.buffer +=1

        if self.b1 is None and self.b2 is None:
            # Initialize the first point
            self.b1 = math.floor(Value / self.epsilon) * self.epsilon
            self.b2 = math.ceil(Value / self.epsilon) * self.epsilon
            return



        for(tc,vc) in data_signal[1:]:
            if vc>slp_upper_1*(tc-ts)+b1+self.epsilon or vc<slp_lower_1*(tc-ts)+b1-self.epsilon: #check flor
                floor = False
            if vc>slp_upper_2*(tc-ts)+b2+self.epsilon or vc<slp_lower_2*(tc-ts)+b2-self.epsilon: #check ceil
                ceil = False
            length = length + floor - ceil # +1 when floor = True , -1 when ceil = True
            if not floor and not ceil: # end of 1 segment
                if length >0:
                    if b1 not in self.b_intervals:
                        self.b_intervals[b1] = []
                    self.b_intervals[b1].append((slp_lower_1,slp_upper_1,ts))
                else:
                    if b2 not in self.b_intervals:
                        self.b_intervals[b2] = []
                    self.b_intervals[b2].append((slp_lower_2,slp_upper_2,ts))

                ts,vs=tc,vc
                b1 = math.floor(vs / self.epsilon) * self.epsilon
                b2 = math.ceil(vs / self.epsilon) * self.epsilon
                slp_upper_1, slp_lower_1 = float('inf'), float('-inf')  # handle floor
                slp_upper_2, slp_lower_2 = float('inf'), float('-inf')  # handle ceil
                floor = True
                ceil = True
                length = 0
                continue
            #update the upper, lower slope of floor and ceil boundary
            if vc < slp_upper_1*(tc-ts)+b1-self.epsilon:
                slp_upper_1= round((vc+self.epsilon-b1)/(tc-ts),2)
            if vc > slp_lower_1*(tc-ts) + b1+self.epsilon:
                slp_lower_1 = round((vc-self.epsilon-b1)/(tc-ts),2)
            if vc < slp_upper_2*(tc-ts)+b2-self.epsilon:
                slp_upper_2 = round((vc+self.epsilon-b2)/(tc-ts),2)
            if vc > slp_lower_2*(tc-ts)+b2+self.epsilon:
                slp_lower_2 = round((vc-self.epsilon-b2)/(tc-ts),2)

        if length  >0 :
            if b1 not in self.b_intervals:
                self.b_intervals[b1] = []
            self.b_intervals[b1].append((slp_lower_1,slp_upper_1,ts));
        else:
            if b2 not in self.b_intervals:
                self.b_intervals[b2] = []
            self.b_intervals[b2].append((slp_lower_2,slp_upper_2,ts));

        if self.buffer >= self.buffer_limit:
            print(f'Buffer limit reached: {self.buffer}')
            self.tmp_b_intervals = self.b_intervals
            self.buffer = 0
            self.b_intervals={}
            self.mix_piece_phase2()

    def mix_piece_phase2(self):
        groups_b=[]
        ungrouped_b=[]

        for bi,intervals_bi in self.tmp_b_intervals.items():
            group =(bi,float('-inf'),float('inf'),[])  # (b,lowwer_slope,upper_slope,[timeStamp])
            intervals_bi.sort(key=lambda x: x[0]) #sort by lower slope | intervals_bi(lower,upper,ts)
            for interval in intervals_bi:
                #If interval lower slope <= group upper slope and interval upper slope > group lower slope
                #Update group upper|lower of group min(upper) max (lower)
                if interval[0] <= group[2] and interval [1]>=group[1]:
                    new_upper_slope=min(group[2],interval[1])
                    new_lower_slope=max(group[1],interval[0])
                    group[3].append(interval[2])
                    group=(bi,new_lower_slope,new_upper_slope,group[3])
                elif len(group[3])>1:  #
                    groups_b.append(group)
                    group = (bi, interval[0], interval[1], [interval[2]])
                else: #means that the group[3] just have 1 element , we can se that at an interval (bi,lower,upper,time_start)
                    ungrouped_b.append(group)
                    group = (bi, interval[0], interval[1], [interval[2]])
            if len(group[3])>1:
                groups_b.append(group)
            else:
                ungrouped_b.append(group)

        groups = []
        rest = []
        group = (float('-inf'),float('inf'),[]) #lower_slp , upper_slp, array of <quantitize,start time>
        ungrouped_b.sort(key=lambda x: x[1]) #sort by lower slope

        for interval in ungrouped_b: #interval in ungrouped is a group that time stamp have 1 element (bi,lower,upper,[start_time])
            if interval[1]<group[1] and interval[2]>group[0]:
                new_upper_slope=min(group[1],interval[2])
                new_lower_slope=max(group[0],interval[1])
                tmp_bt = (interval[0],interval[3][0])
                group_bt = group[2]+ [tmp_bt]
                group = (new_lower_slope,new_upper_slope,group_bt)
            elif len(group[2])>1:
                groups.append(group)
                group_bt=[(interval[0],interval[3][0])]
                group=(interval[1],interval[2],group_bt)
            else:
                rest.append(group)
                group_bt = [(interval[0], interval[3][0])]
                group = (interval[1], interval[2], group_bt)
        if len(group[2])>1:
            groups.append(group)
        else:
            rest.append(group)

        reformat_groups_b=[]
        reformat_groups=[]
        reformat_rest=[]

        for group_b in groups_b:
            tmp_group_b=(group_b[0],round((group_b[1]+group_b[2])/2,2),group_b[3])
            reformat_groups_b.append(tmp_group_b)
        for group in groups:
            tmp_group=(round((group[0]+group[1])/2,2),group[2])
            reformat_groups.append(tmp_group)
        for group in rest:
            tmp_rest=(round((group[0]+group[1])/2,2),group[2])
            reformat_rest.append(tmp_rest)



        save_groups_b_to_csv(reformat_groups_b,output_file_path='../result/mix-piece/MP_Compress_Groups_b.csv')
        save_groups_to_csv(reformat_groups,output_file_path='../result/mix-piece/MP_Compress_Groups.csv')
        save_groups_to_csv(reformat_rest,output_file_path='../result/mix-piece/MP_Compress_Rest.csv')
        return reformat_groups_b,reformat_groups,reformat_rest

    def Mix_Piece_Compress_And_Decompress(self,data):
        result_phase1 = self.mix_piece_phase1(data)
        groups_b,groups,rest=self.mix_piece_phase2(result_phase1)
        self.decompres_mix_piece_from_phases1_result(result_phase1)



file_path = "../streaming_data.csv"
data = load_data(file_path)
epsilon = 5
MP_compressor = TimeSeriesCompressionMixPiece(epsilon)
MP_compressor.Mix_Piece_Compress_And_Decompress(data)





