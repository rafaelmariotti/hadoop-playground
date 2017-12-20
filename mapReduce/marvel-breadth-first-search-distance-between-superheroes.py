from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class Node:
    def __init__(self):
        self.characterID = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'

    # line format is ID|friends|distance|color
    def fromLine(self, line):
        fields = line.split('|')
        if (len(fields) == 4):
            self.characterID = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]

    def getLine(self):
        connections = ','.join(self.connections)
        return '|'.join((self.characterID,
                         connections,
                         str(self.distance),
                         self.color))


class MRBFSIteration(MRJob):

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRBFSIteration, self).configure_options()
        self.add_passthrough_option(
            '--target', help="Character ID we are searching for")

    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)

        if (node.color == 'GRAY'):
            for connection in node.connections:
                vnode = Node()
                vnode.characterID = connection
                vnode.distance = int(node.distance) + 1
                vnode.color = 'GRAY'

                # if we find our target hero
                if (self.options.target == connection):
                    counterName = ("Target ID " + connection +
                                   " was hit with distance " +
                                   str(vnode.distance))
                    self.increment_counter('Degrees of Separation',
                                           counterName,
                                           1)

                yield connection, vnode.getLine()

            # mark as a reached node
            node.color = 'BLACK'

        # yield the input node so this way we don't lose it
        yield node.characterID, node.getLine()

    def reducer(self, key, values):
        friends = []
        distance = 9999
        color = 'WHITE'

        for value in values:
            node = Node()
            node.fromLine(value)

            if (len(node.connections) > 0):
                friends.extend(node.connections)

            if (node.distance < distance):
                distance = node.distance

            if (node.color == 'BLACK'):
                color = 'BLACK'

            if (node.color == 'GRAY' and color == 'WHITE'):
                color = 'GRAY'

        node = Node()
        node.characterID = key
        node.distance = distance
        node.color = color
        node.connections = friends[:500]

        yield key, node.getLine()

if __name__ == '__main__':
    MRBFSIteration.run()