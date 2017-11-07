from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(#mapper=self.mapper_passthrough,
                   reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield 'dummy key', (sum(values), key)

    # dummy method (just to avoid bugs with mrjob)
    # def mapper_passthrough(self, key, values):
    #    yield key, values

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
