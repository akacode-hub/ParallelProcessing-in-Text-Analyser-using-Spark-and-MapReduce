import sys
import argparse
import findspark
findspark.init()
from time import time
from pyspark import SparkContext
from helpers import to_lower_case ,strip_non_alpha,find_match,create_list_from_file


def count_sentences(rdd):
    """ Count the sentences in a file.

    Input:
    - rdd: an RDD containing the contents of a file, with one sentence in each element.

    
    Return value: The total number of sentences in the file.
    """

    return rdd.count() 

def count_words(rdd):
    """ Count the number of words in a file.

    Input:
    - rdd: an RDD containing the contents of a file, with one sentence in each element.

    
    Return value: The total number of words in the file.
    """

    return len(rdd.split())

def compute_counts(rdd,numPartitions=10):
    """ Produce an rdd that contains the number of occurences of each word in a file.

    Each word in the file is converted to lowercase and then stripped of leading and trailing non-alphabetic
    characters before its occurences are counted.
    
    Input:
    - rdd: an RDD containing the contents of a file, with one sentence in each element.

    
    Return value: an RDD containing pairs of the form (word,count), where word is is a lowercase string, 
    without leading or trailing non-alphabetic characters, and count is the number of times it appears
    in the file. The returned RDD should have a number of partitions given by numPartitions.

    """
    # Reference taken from lecture 2,3,4,ppt taught by proffessor 
    rdd2 = rdd.flatMap(lambda rdd: rdd.split())\
               .map(lambda rdd: to_lower_case(rdd))\
               .map(lambda rdd :strip_non_alpha(rdd))
    newrdd=rdd2.map(lambda word: (word, 1))\
              .reduceByKey(lambda x, y: x + y,numPartitions)     
    return newrdd
    

def count_difficult_words(counts,easy_list):
    """ Count the number of difficult words in a file.

    Input:
    - counts: an RDD containing pairs of the form (word,count), where word is a lowercase string, 
    without leading or trailing non-alphabetic characters, and count is the number of times this word appears
    in the file.
    - easy_list: a list of words deemed 'easy'.
    Return value: the total number of 'difficult' words in the file represented by RDD counts. 

    A word should be considered difficult if is not the 'same' as a word in easy_list. Two words are the same
    if one is the inflection of the other, when ignoring cases and leading/trailing non-alphabetic characters. 
    """
    #print(type(easy_list)) #Reference taken from the lecture ppt that explained filter operation 
    return counts.filter(lambda a: find_match(a[0], easy_list) is None).map(lambda a: a[1]).sum()
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = 'Text Analysis via the Dale Chall Formula',formatter_class=argparse.ArgumentDefaultsHelpFormatter)    
    parser.add_argument('mode', help='Mode of operation',choices=['SEN','WRD','UNQ','TOP20','DFF','DCF']) 
    parser.add_argument('input', help='Text file to be processed. This file contains text over several lines, with each line corresponding to a different sentence.')
    parser.add_argument('--master',default="local[20]",help="Spark Master")
    parser.add_argument('--N',type=int,default=20,help="Number of partitions to be used in RDDs containing word counts.")
    parser.add_argument('--simple_words',default="DaleChallEasyWordList.txt",help="File containing Dale Chall simple word list. Each word appears in one line.")
    args = parser.parse_args()
    

    sc = SparkContext(args.master, 'Text Analysis')
    rdd = sc.textFile(args.input).repartition(args.N)

    if args.mode == 'SEN': 
        num_sentences = count_sentences(rdd)
        print("Number of sentences:", num_sentences)

    elif args.mode == 'WRD':
        num_words = rdd.flatMap(lambda line: line.split()).count()
        print("Number of words:", num_words)

    elif args.mode == 'UNQ':
        no_of_uniquewords=compute_counts(rdd,args.N)
        print("Number of unique words : ",no_of_uniquewords.count())

    elif args.mode =='TOP20':
        #Reference from proffessor's lecture 3 and 4 ppt  
        top20 = compute_counts(rdd,args.N).sortBy(lambda pair:pair[1],ascending = False).take(20)
        print("The top 20 records :",top20)

    elif args.mode =='DFF':

        counts=compute_counts(rdd,args.N)
        simple_words = create_list_from_file(args.simple_words)
        no_of_difficultwords = count_difficult_words(counts,simple_words)
        print("The number of difficult words are :",no_of_difficultwords)

    elif args.mode=='DCF':
        start = time()
        num_sentences = count_sentences(rdd)
        num_words = rdd.flatMap(lambda line: line.split()).count()
        counts=compute_counts(rdd,args.N)
        simple_words = create_list_from_file(args.simple_words)
        no_of_difficultwords = count_difficult_words(counts,simple_words)
        a= no_of_difficultwords/num_words
        b=num_words/num_sentences
        c = float((0.1579*(a*100))+(0.0496*(b)))
        print(" The Dale-Chall Formula is : ",c)
        end = time()
    print('Total execution time:',str(end-start)+'sec')
    sc.stop()

 
    
   
    



