import csv
import os
from math import floor

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

def save_groups_to_csv(groups, output_file_path):
    # Convert the groups to a DataFrame
    df = pd.DataFrame(groups, columns=['Quantized Value', 'Lower Slope', 'Upper Slope', 'Time Points'])

    # Check if the file exists to determine if the header should be written
    if not os.path.isfile(output_file_path):
        # File doesn't exist, write with header
        df.to_csv(output_file_path, mode='w', index=False, header=True)
    else:
        # File exists, append without header
        df.to_csv(output_file_path, mode='a', index=False, header=False)


class TimeSeriesCompressionSimPiece:
    def __init__(self, epsilon):
        self.epsilon = epsilon
        self.dict_segments = {}
        self.tmp_dict_segments = {}
        self.buffer = 0
        self.slp_upper = float('inf')
        self.slp_lower = float('-inf')
        self.pb = None
        self.buffer_limit = 1000  # Set the buffer size to 1000 points for testing
        self.tc = None

    def process_data_point(self, data_point):
        """
        Process a single data point with Sim-piece phrase 1, and check if buffer limit is reached
        to apply phrase 2.
        :param data_point: (TimeStamp, Value)
        """
        TimeStamp, Value = data_point
        self.buffer += 1

        if self.pb is None:
            # Initialize the first point
            self.pb = (TimeStamp, floor(Value / self.epsilon) * self.epsilon)
            self.tc = TimeStamp
            return  # No processing for the first point

        # Update the closing time
        self.tc = TimeStamp

        # Compute the bounds for the next point
        tmp1 = self.slp_upper * (TimeStamp - self.pb[0]) + self.pb[1] + self.epsilon
        tmp2 = self.slp_lower * (TimeStamp - self.pb[0]) + self.pb[1] - self.epsilon

        # If the new point violates the bounds, create a new segment
        if Value > tmp1 or Value < tmp2:
            if self.pb[1] not in self.dict_segments:
                self.dict_segments[self.pb[1]] = []
            self.dict_segments[self.pb[1]].append((round(self.slp_upper, 3), round(self.slp_lower, 3), self.pb[0]))

            # Reset slopes and set the new base point
            self.pb = (TimeStamp, floor(Value / self.epsilon) * self.epsilon)
            self.slp_upper = float('inf')
            self.slp_lower = float('-inf')
        else:
            # Update slopes
            if Value < self.slp_upper * (TimeStamp - self.pb[0]) + self.pb[1] - self.epsilon:
                self.slp_upper = (Value + self.epsilon - self.pb[1]) / (TimeStamp - self.pb[0])

            if Value > self.slp_lower * (TimeStamp - self.pb[0]) + self.pb[1] + self.epsilon:
                self.slp_lower = (Value - self.epsilon - self.pb[1]) / (TimeStamp - self.pb[0])

        # Check if the buffer has reached the limit
        if self.buffer >= self.buffer_limit:
            # Apply phrase 2 and reset the buffer
            print(f'Buffer limit reached: {self.buffer}')
            self.tmp_dict_segments = self.dict_segments
            self.dict_segments = {}
            self.buffer = 0
            self.process_phrase_2()

    def process_phrase_2(self):
        """
        Apply Sim-piece phrase 2 to the current segments and reset the buffer.
        """
        groups = []
        for b, segs in self.tmp_dict_segments.items():
            grp = (b, float('inf'), float('-inf'), [])  # (b, slp_upper, slp_lower, [array of timestamp])
            segs.sort(key=lambda x: x[1])  # Sort by lower slope in ascending order

            for seg in segs:
                seg_upper_slope, seg_lower_slope, seg_t = seg

                if seg_upper_slope >= grp[2] and seg_lower_slope <= grp[1]:
                    grp = (
                        b,
                        min(grp[1], seg_upper_slope),
                        max(grp[2], seg_lower_slope),
                        grp[3] + [seg_t]
                    )
                else:
                    # If not group-able, finalize the current group and start a new one
                    groups.append(grp)
                    grp = (b, seg_upper_slope, seg_lower_slope, [seg_t])

            # Append the last group
            groups.append(grp)

        # Save data to a CSV file
        save_groups_to_csv(groups, "../../result/sim-piece/SP_Compress.csv")
        self.tmp_dict_segments = {}

        print(f"Processed {len(groups)} groups from the buffer.")

    def finish_processing(self):
        """
        Call this method to process any remaining data points in the buffer after all points have been processed.
        """
        if self.buffer > 0:
            # If there are still points left in the buffer, process them
            print(f"Processing remaining data in buffer: {self.buffer}")
            # add current segment havenot been add to the dic_segments
            if self.pb[1] not in self.dict_segments:
                self.dict_segments[self.pb[1]] = []
            self.dict_segments[self.pb[1]].append((round(self.slp_upper, 3), round(self.slp_lower, 3), self.pb[0]))


            self.tmp_dict_segments = self.dict_segments
            self.process_phrase_2()


file_path = "../../generate_data/streaming_data.csv"
data = load_data(file_path)
epsilon = 5
SP_Compressor = TimeSeriesCompressionSimPiece(epsilon)

for data_point in data :
    SP_Compressor.process_data_point(data_point)
SP_Compressor.finish_processing()