from confluent_kafka import Consumer
from algorithm.Sim_Piece.SP_Compress import TimeSeriesCompressionSimPiece
from algorithm.Mix_Piece.MP_Compress import TimeSeriesCompressionMixPiece

configs = {'bootstrap.servers': '127.0.0.1:9092',
           'group.id': 'my-python-application',
           'enable.auto.commit': True}
consumer = Consumer(configs)

consumer.subscribe(['demo_python'])
epsilon = 5
compressor_SP = TimeSeriesCompressionSimPiece(epsilon)
compressor_MP = TimeSeriesCompressionMixPiece(epsilon)
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8') if msg.value() else None
        print(f"Consumed message: Key={key}, Value={value}, "
              f"Partition={msg.partition()}, Offset={msg.offset()}")
        datapoint=(int(key), float(value))
        compressor_SP.process_data_point(datapoint)
        compressor_MP.mix_piece_phase1(datapoint)

except KeyboardInterrupt:
    compressor_SP.finish_processing()
    compressor_MP.finish_processing()
    print("Consumer interrupted, closing...")

finally:
    consumer.close()
