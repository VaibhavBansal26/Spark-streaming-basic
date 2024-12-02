from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def generate_kmers(line, k=3):
    kmers = []
    words = line.strip().split()
    for word in words:
        word = word.lower()
        if len(word) >= k:
            for i in range(len(word) - k + 1):
                kmers.append(word[i:i+k])
        else:
            kmers.append(word) 
    return kmers

def updateFunction(new_values, running_count):
    if running_count is None:
        running_count = 0
    return sum(new_values) + running_count

if __name__ == "__main__":
    # "local[2] = 2 threads to be used for local execution"
    # "StreamingContext(sc, 10) = 10 is the batch interval in seconds"
    # DS stream is created from the socket on localhost:9999
    # k mer of length 3 is generated from each line
    conf = SparkConf().setMaster("local[2]").setAppName("K-mer Count App")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint_directory")
    lines = ssc.socketTextStream("localhost", 9999)
    kmers = lines.flatMap(lambda line: generate_kmers(line, 3))
    kmer_pairs = kmers.map(lambda kmer: (kmer, 1))
    kmer_counts = kmer_pairs.updateStateByKey(updateFunction)
    kmer_counts.pprint(num=100)
    ssc.start()
    ssc.awaitTermination()
