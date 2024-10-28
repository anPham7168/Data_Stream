import csv
import math

class TimeSeriesCompressionSimPiece:
    def __init__(self,epsilon):
        self.epsilon = epsilon # temp dictionary use for storing result of phase 1

    def load_data (self,file_path):
        data_signal =[]
        with open(file_path,'r') as file:
            reader = csv.reader(file)
            for row in reader:
                timestamp = int(row[0])
                value = float(row[1])
                data_signal.append((timestamp, value))
        return data_signal

    def mix_piece_phase1(self,data_signal):
        b_intervals ={}
        ts,vs = data_signal[0] # the first point of data stream
        b1 = math.floor(vs/self.epsilon)*self.epsilon
        b2 = math.ceil(vs/self.epsilon)*self.epsilon
        slp_upper_1,slp_lower_1= float('inf'),float('-inf') #handle floor
        slp_upper_2 ,slp_lower_2 = float('inf'),float('-inf') #handle ceil
        floor = True
        ceil = True
        length = 0

        for(tc,vc) in data_signal[1:]:
            if vc>slp_upper_1*(tc-ts)+b1+self.epsilon or vc<slp_lower_1*(tc-ts)+b1-self.epsilon: #check flor
                floor = False
            if vc>slp_upper_2*(tc-ts)+b2+self.epsilon or vc<slp_lower_2*(tc-ts)+b2-self.epsilon: #check ceil
                ceil = False
            length = length + floor - ceil # +1 when floor = True , -1 when ceil = True
            if not floor and not ceil: # end of 1 segment
                if length >0:
                    if b1 not in b_intervals:
                        b_intervals[b1] = []
                    b_intervals[b1].append((slp_lower_1,slp_upper_1,ts))
                else:
                    if b2 not in b_intervals:
                        b_intervals[b2] = []
                    b_intervals[b2].append((slp_lower_2,slp_upper_2,ts))

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
                slp_upper_1= (vc+self.epsilon-b1)/(tc-ts)
            if vc > slp_lower_1*(tc-ts) + b1+self.epsilon:
                slp_lower_1 = (vc-self.epsilon-b1)/(tc-ts)
            if vc < slp_upper_2*(tc-ts)+b2-self.epsilon:
                slp_upper_2 = (vc+self.epsilon-b2)/(tc-ts)
            if vc > slp_lower_2*(tc-ts)+b2+self.epsilon:
                slp_lower_2 = (vc-self.epsilon-b2)/(tc-ts)

        if length  >0 :
            if b1 not in b_intervals:
                b_intervals[b1] = []
            b_intervals[b1].append((slp_lower_1,slp_upper_1,ts));
        else:
            if b2 not in b_intervals:
                b_intervals[b2] = []
            b_intervals[b2].append((slp_lower_2,slp_upper_2,ts));

        return b_intervals

    def mix_piece_phase2(self,b_intervals):
        groups_b=[]
        ungrouped_b=[]

        for bi,intervals_bi in b_intervals.items():
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
        group = (float('-inf'),float('inf'),[]) #lower_slp , upper_slp,
        ungrouped_b.sort(key=lambda x: x[1]) #sort by lower slope

        for interval in ungrouped_b: #interval in ungrouped is a group that time stamp have 1 element (bi,lower,upper,[start_time])
            if interval[1]<group[1] and interval[2]>group[0]:
                new_upper_slope=min(group[1],interval[2])
                new_lower_slope=max(group[0],interval[1])
                arr_time = group[2]+interval[3]
                group = (new_lower_slope,new_upper_slope,arr_time)
            elif len(group[2])>1:
                groups.append(group)
                group=(interval[1],interval[2],interval[3])
            else:
                rest.append(group)
                group = (interval[1], interval[2], interval[3])
        if len(group[2])>1:
            groups.append(group)
        else:
            rest.append(group)

        return groups_b,groups,rest

    def decompres_mix_piece(self, groups_b, groups, rest):
        decompressed_data = []

        # Process each group in groups_b (common starting points)
        for group in groups_b:
            b, al, au, timestamps = group
            mid_slope = (al + au) / 2  # Middle slope approximation
            for t in timestamps:
                # Approximate value using the mid-slope
                approx_value = b + mid_slope * (t - timestamps[0])
                decompressed_data.append((t, approx_value))

        # Process each group in groups (flexible starting points)
        for group in groups:
            al, au, time_data = group
            mid_slope = (al + au) / 2
            for b, t in time_data:
                approx_value = b + mid_slope * (t - time_data[0][1])
                decompressed_data.append((t, approx_value))

        # Process ungrouped intervals in rest
        for group in rest:
            al, au, time_data = group
            mid_slope = (al + au) / 2
            for b, t in time_data:
                approx_value = b + mid_slope * (t - time_data[0][1])
                decompressed_data.append((t, approx_value))

        # Sort decompressed data by timestamps for ordered output
        decompressed_data.sort(key=lambda x: x[0])
        return decompressed_data





