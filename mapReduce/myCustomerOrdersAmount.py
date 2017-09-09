from mrjob.job import MRJob

class MyCustomerOrdersAmount(MRJob):
	
	def mapper(self, _, line):
		(customer_id, amount, value) = line.split(',')
		yield customer_id, int(amount)*float(value)


	def reducer(self, customer_id, total_value):
		yield customer_id, round(sum(total_value), 2)


if __name__ == '__main__':
	MyCustomerOrdersAmount.run()