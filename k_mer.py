from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def generate_kmers(line, k=3):
    kmers = []
    words = line.strip().split()
    for word in words:
        for i in range(len(word) - k + 1):
            kmers.append(word[i:i+k])
    return kmers

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[2]").setAppName("K-mer Count")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    lines = ssc.socketTextStream("localhost", 9999)

    kmers = lines.flatMap(lambda line: generate_kmers(line, 3))

    kmer_counts = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b)

    kmer_counts.pprint()
    print("`kmer_counts` is a DStream object of type `TransformedDStream`")

    ssc.start()
    ssc.awaitTermination()
