#b. Which pickup location generates the most revenue?

from mrjob.job import MRJob
from mrjob.step import MRStep

class PickupLocationMostRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_revenue, reducer=self.reducer_sum_revenue),
            MRStep(reducer=self.reducer_find_max_revenue)
        ]

    def mapper_get_revenue(self, _, line):
        """Mapper to extract revenue and pickup location from each line."""
        if not line.startswith('VendorID'):  # Skip header line
            fields = line.split(',')
            try:
                pickup_location = fields[7].strip()  # PULocationID (assumed to be the 8th field)
                revenue = float(fields[16].strip())  # total_amount (assumed to be the 17th field)
                yield pickup_location, revenue  # Emit (pickup_location, revenue)
            except (ValueError, IndexError):
                pass  # Skip lines with invalid data

    def reducer_sum_revenue(self, pickup_location, revenues):
        """Reducer to sum up revenue for each pickup location."""
        total_revenue = sum(revenues)
        yield None, (total_revenue, pickup_location)  # Emit (None, (total_revenue, pickup_location))

    def reducer_find_max_revenue(self, _, location_revenue_pairs):
        """Reducer to find the pickup location with the highest revenue."""
        max_revenue = 0
        top_location = None

        for total_revenue, pickup_location in location_revenue_pairs:
            if total_revenue > max_revenue:
                max_revenue = total_revenue
                top_location = pickup_location

        yield top_location, max_revenue  # Emit (pickup_location, max_revenue)

if __name__ == '__main__':
    PickupLocationMostRevenue.run()
