import csv
import pandas as pd
from matplotlib import pyplot as plt

def import_data_from_csv_file(file_path, list_field):
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Ensure that the provided fields exist in the DataFrame
    missing_fields = [field for field in list_field if field not in df.columns]
    if missing_fields:
        raise ValueError(f"Fields {missing_fields} not found in the CSV file")

    # Extract the specified columns and zip them into tuples
    data = list(zip(*(df[field] for field in list_field)))

    return data
def extract_elements(compressiveData):
    extracted_tuples = []
    for tup in compressiveData:
        # Extract the first three elements
        first_three = tup[:3]
        # Convert the fourth element to a list of integers
        int_list = eval(tup[3])
        # Create a new tuple for each element in the list
        for item in int_list:
            new_tup = first_three + (item,)
            extracted_tuples.append(new_tup)
    return extracted_tuples
def procces_slope(extracted_tuples):
    result = []
    for tup in extracted_tuples:
        result.append((tup[0], round((tup[1]+tup[2])/float(2),2), tup[3]))
    return result
def reconstruct_data(segments):
    x_vals = []
    y_vals = []

    # Loop through each segment
    for i in range(len(segments)):
        y_start, slope, t_start = segments[i]
        if i < len(segments) - 1:
            t_end = int(segments[i + 1][2])  # Use int() to convert the float to an integer
        else:
            t_end = t_start + 10  # You can also ensure this is an integer

        # Generate data points for this segment
        t_range = range(int(t_start), int(t_end))  # Convert to integer
        for t in t_range:
            y = y_start + slope * (t - t_start)
            x_vals.append(t)
            y_vals.append(y)

    return x_vals, y_vals




file_name = "../result/SP_Compress.csv"
list_field = ["Quantized Value","Lower Slope","Upper Slope","Time Points"]

compressiveData = import_data_from_csv_file(file_name, list_field)
extracted_tuples = extract_elements(compressiveData)
extracted_tuples_slope_processed = procces_slope(extracted_tuples)
extracted_tuples_slope_processed.sort(key=lambda tup: tup[2])
for tup in extracted_tuples_slope_processed:
    print(f'{tup}')
x_vals, y_vals = reconstruct_data(extracted_tuples_slope_processed)

plt.plot(x_vals, y_vals, linestyle='-')
plt.xlabel('Time')
plt.ylabel('Value')
plt.title('Reconstructed Time Series Data from Compressive Segments')
plt.grid(True)
plt.show()

# Specify the output file
output_file = "../result/SP_Decompress.csv"

# Open the CSV file in write mode
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)

    # Write the header
    writer.writerow(['Timestamp', 'Value'])

    # Write the data (assuming x_vals corresponds to timestamps and y_vals to values)
    for x, y in zip(x_vals, y_vals):
        writer.writerow([x, round(y, 2)])  # Round values to 2 decimal places

print(f"Decompressed data has been written to {output_file}")