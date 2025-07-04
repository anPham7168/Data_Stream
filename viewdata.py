import pandas as pd
import matplotlib.pyplot as plt

# Function to load data from CSV
def load_csv_data(file_name):
    return pd.read_csv(file_name)

# Load data from the two CSV files
streaming_data = load_csv_data('generate_data/streaming_data.csv')  # Update with the correct file path
decompressed_data_MP=load_csv_data('./result/mix-piece/MP_Decompress.csv')
decompressed_data_SP = load_csv_data('result/sim-piece/SP_Decompress.csv')  # Update with the correct file path


#Plot the graph comparing original streaming data and decompressed data
plt.figure(figsize=(10, 6))
plt.plot(streaming_data['Timestamp'], streaming_data['Value'], label='Original Streaming Data', color='blue')
plt.plot(decompressed_data_MP['Timestamp'], decompressed_data_MP['Value'], label='Decompressed Streaming Data MP ', color='red', )
plt.plot(decompressed_data_SP['Timestamp'], decompressed_data_SP['Value'], label='Decompressed Streaming Data SP ', color='green', )

# Set titles and labels
plt.title('Comparison of Original Streaming Data and Decompressed Data')
plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.legend()

# Show the plot
plt.grid(True)
plt.show()