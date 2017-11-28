from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperHero(MRJob):
    heroNames = {}

    def configure_options(self):
        super(MostPopularSuperHero, self).configure_options()
        self.add_file_option('--names', help='Path to marvel-names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper_init=self.mapper_load_name_dictionary,
                   mapper=self.mapper_prep_for_sort,
                   reducer=self.reducer_find_max_friends)
        ]

    def mapper_count_friends_per_line(self, _, line):
        heroesList = line.split()
        heroID = heroesList[0]
        totalFriends = len(heroesList) - 1
        yield int(heroID), int(totalFriends)

    def reducer_combine_friends(self, heroID, totalFriends):
        yield heroID, sum(totalFriends)

    def mapper_load_name_dictionary(self):
        with open("marvel-names.txt", encoding='ascii', errors='ignore') as file:
            for line in file:
                field = line.split('"')

                heroID = int(field[0])
                heroName = field[1]
                self.heroNames.update({heroID: heroName})

    def mapper_prep_for_sort(self, heroID, totalFriends):
        heroName = self.heroNames[heroID]
        yield 'dummy key', (totalFriends, heroName)

    def reducer_find_max_friends(self, key, value):
        yield max(value)

if __name__ == '__main__':
    MostPopularSuperHero.run()
