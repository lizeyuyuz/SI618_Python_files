#!/usr/bin/python

'''
An old version was created by Dr. Yuhang Wang
'''

import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b\w+'*\w*\b")

class BigramCount(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
  
    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for b in zip(words[:-1], words[1:]):
            bigram = " ".join(b)
            yield (bigram.lower(), 1)

    def combiner(self, bigram, counts):
        yield (bigram, sum(counts))
         
    def reducer(self, bigram, counts):
        yield (bigram, str(sum(counts)))

if __name__ == '__main__':
    BigramCount.run()

