from mrjob.job import MRJob
from mrjob.step import MRStep


class MyCustomerOrdersAmount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_customer_by_value,
                   reducer=self.reducer_customer_sum_total_value),
            MRStep(mapper=self.mapper_value_by_customer,
                   reducer=self.reducer_customer_by_total_value)
        ]

    def mapper_customer_by_value(self, _, line):
        (customer_id, item_id, value) = line.split(',')
        yield customer_id, float(value)

    def reducer_customer_sum_total_value(self, customer_id, total_value):
        yield customer_id, round(sum(total_value), 2)

    def mapper_value_by_customer(self, customer, total_value):
        yield '%04.02f' % float(total_value), customer

    def reducer_customer_by_total_value(self, total_value, customers):
        for customer in customers:
            yield customer, total_value


if __name__ == '__main__':
    MyCustomerOrdersAmount.run()
