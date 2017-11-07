from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    movie = {}

    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(#mapper=self.mapper_passthrough,
                   reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_init(self):
        with open("u.item", encoding='ascii', errors='ignore') as file:
            for line in file:
                field = line.split('|')
                movieID = field[0]
                movieName = field[1]

                self.movie.update({movieID: movieName})

    def reducer_count_ratings(self, key, values):
        yield 'dummy key', (sum(values), self.movie[key])

    # dummy method (just to avoid bugs with mrjob)
    # def mapper_passthrough(self, key, value):
    #    yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
