from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations


class MovieSimilarities(MRJob):

    def configure_options(self):
        super(MovieSimilarities, self).configure_options()
        self.add_file_option('--movies', help='Path to u.item file')

    def load_movie_names(self):
        # Load database of movie names.
        self.movie_names = {}

        with open("u.item", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movie_names[int(fields[0])] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                   reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                   reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                   mapper_init=self.load_movie_names,
                   reducer=self.reducer_output_similarities)]

    def mapper_parse_input(self, key, line):
        # mapper tuples of (movie_id, rating) for all users
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield user_id, (movie_id, float(rating))

    def reducer_ratings_by_user(self, user_id, movie_ratings):
        # reduce (group) all (movie_id, rating) tuple in an array for all users
        ratings = []

        for movie_id, rating in movie_ratings:
            ratings.append((movie_id, rating))

        yield user_id, ratings

    def mapper_create_item_pairs(self, user_id, movie_ratings):
        # for each user, combine all movie tuple possibilities
        for movie_rating1, movie_rating2 in combinations(movie_ratings, 2):
            movie_id1 = movie_rating1[0]
            rating1 = movie_rating1[1]
            movie_id2 = movie_rating2[0]
            rating2 = movie_rating2[1]

            # yield both orders so similarities are bi-directional
            yield (movie_id1, movie_id2), (rating1, rating2)
            yield (movie_id2, movie_id1), (rating2, rating1)

    def reducer_compute_similarity(self, movie_pair, rating_pairs):
        # calculate similarity and return all combinations
        # where score and ratings are higher than a minimum value
        score, num_pairs = self.cosine_similarity(rating_pairs)

        # guarantee a minimum score and a minimum number of co-ratings
        if (num_pairs > 10 and score > 0.95):
            yield movie_pair, (score, num_pairs)

    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two rating vectors
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def mapper_sort_similarities(self, movie_pair, scores_and_num_pairs):
        # translate movie IDs to movie names, organizing our data
        score, num_pair = scores_and_num_pairs
        movie1, movie2 = movie_pair

        yield (self.movie_names[int(movie1)], score), \
            (self.movie_names[int(movie2)], num_pair)

    def reducer_output_similarities(self, movie_score, similarity):
        # output result where it shows the movie name and the related movie,
        # along with the score and how many people scored it
        movie1, score = movie_score
        for movie2, num_pair in similarity:
            yield movie1, (movie2, score, num_pair)


if __name__ == '__main__':
    MovieSimilarities.run()
