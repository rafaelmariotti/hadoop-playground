from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")


class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_repeated_words,
                   reducer=self.reducer_total_words),
            MRStep(mapper=self.mapper_sort_words,
                   reducer=self.reducer_sort_words)
        ]

    def mapper_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def combiner_repeated_words(self, words, total_words):
        yield words, sum(total_words)

    def reducer_total_words(self, words, total_words):
        yield words, sum(total_words)

    def mapper_sort_words(self, words, total_words):
        yield '%10.0f' % int(total_words), words

    def reducer_sort_words(self, total_words, words):
        for word in words:
            yield word, total_words.strip()


if __name__ == '__main__':
    MRWordFrequencyCount.run()
