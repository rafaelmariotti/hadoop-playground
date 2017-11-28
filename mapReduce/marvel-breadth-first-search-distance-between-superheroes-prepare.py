# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.

import sys

heroBaseID = sys.argv[1]
print("Creating BFS starting input for character {}".format(heroBaseID))

with open("dataset/marvelHeroes/breadth-first-search-{}.txt"
          .format(heroBaseID), 'w') as outputFile:

    with open("dataset/marvelHeroes/marvel-graph.txt") as file:
        for line in file:
            fields = line.split()
            heroID = fields[0]
            numConnections = len(fields) - 1
            connections = fields[-numConnections:]

            color = 'WHITE'
            distance = 9999

            if (heroID == heroBaseID):
                color = 'GRAY'
                distance = 0

            if (heroID != ''):
                friendsID = ','.join(connections)
                outputLine = '|'.join((heroID,
                                       friendsID,
                                       str(distance),
                                       color))
                outputFile.write(outputLine)
                outputFile.write("\n")

    file.close()

outputFile.close()
