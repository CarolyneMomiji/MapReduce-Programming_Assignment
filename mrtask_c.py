#c. What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypeCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_payment_type, reducer=self.reducer_count_payment_types),
            MRStep(reducer=self.reducer_sort_payment_types)
        ]

    def mapper_get_payment_type(self, _, line):
        """Mapper to extract payment type."""
        if not line.startswith('VendorID'):  # Skip header line
            fields = line.split(',')
            try:
                payment_type = fields[9].strip()  # payment_type (assumed to be the 10th field)
                yield payment_type, 1  # Emit (payment_type, 1)
            except IndexError:
                pass  # Skip invalid lines

    def reducer_count_payment_types(self, payment_type, counts):
        """Reducer to sum up counts for each payment type."""
        total_count = sum(counts)
        yield None, (total_count, payment_type)  # Emit (None, (total_count, payment_type))

    def reducer_sort_payment_types(self, _, payment_type_counts):
        """Reducer to sort payment types by count in descending order."""
        sorted_payment_types = sorted(payment_type_counts, reverse=True)  # Sort by count descending
        for total_count, payment_type in sorted_payment_types:
            yield payment_type, total_count  # Emit (payment_type, total_count)

if __name__ == '__main__':
    PaymentTypeCount.run()
