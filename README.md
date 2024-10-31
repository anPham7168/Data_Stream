Welcome to the Data Compression Streaming Project 👋
This project is designed for testing and implementing data compression algorithms on streaming data.

# **Get Started**
## Prepare for Testing

Delete all compressed files in the directories result/mix-piece and result/sim-piece.
Clear data in the decompression files, keeping only the header and ensuring the pointer is set to a new line.

### Test the Algorithm (Without Real-Time Streaming Data)

#### Run the compression scripts:
`python /algorithm/mix-piece/MP_Compress.py`
`python /algorithm/sim-piece/SP_Compress.py`
#### Then, run the decompression scripts:
`python /algorithm/mix-piece/MP_Decompress.py`
`python /algorithm/sim-piece/SP_Decompress.py`
#### Finally, view the compressed data results by running:
`python viewdata.py`

### Test with Real-Time Streaming Data (Using Kafka) 

#### Start the Kafka Consumer
`python /kafka-system/consumer.py`

#### Start the Kafka Producer

`python /kafka-system/producer.py`

Allow the producer to send all generated data to the consumer. Once complete, interrupt the process.

#### Extract and View Data:
Follow the decompression instructions above, then view the streamed data with:

`python viewdata.py`

## Project Structure

algorithm/: Contains the compression and decompression scripts.

kafka-system/: Manages data streaming through Kafka.

result/: Stores output files for compressed and decompressed data.

