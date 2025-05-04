#e. Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep


class AverageTipsToRevenueRatio(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratios, reducer=self.reducer_calculate_totals),
            MRStep(reducer=self.reducer_calculate_averages_and_sort)
        ]

    def mapper_get_ratios(self, _, line):
        """Mapper to extract pickup location, tips, and revenue."""
        if not line.startswith('VendorID'):  # Skip header line
            fields = line.split(',')
            try:
                pickup_location = fields[7].strip()  # PULocationID (assumed to be the 8th field)
                tips = float(fields[14].strip())  # tip_amount (assumed to be the 15th field)
                revenue = float(fields[16].strip())  # total_amount (assumed to be the 17th field)
                if revenue > 0:  # Avoid division by zero
                    tips_to_revenue_ratio = tips / revenue
                    yield pickup_location, (tips_to_revenue_ratio, 1)  # Emit (pickup_location, (ratio, 1))
            except (ValueError, IndexError):
                pass  # Skip lines with invalid data

    def reducer_calculate_totals(self, pickup_location, ratios):
        """Reducer to sum up tips-to-revenue ratios and counts for each pickup location."""
        total_ratio = 0
        total_count = 0
        for ratio, count in ratios:
            total_ratio += ratio
            total_count += count
        yield None, (
        pickup_location, total_ratio, total_count)  # Emit (None, (pickup_location, total_ratio, total_count))

    def reducer_calculate_averages_and_sort(self, _, location_ratios):
        """Reducer to calculate averages and sort results."""
        averages = []
        for pickup_location, total_ratio, total_count in location_ratios:
            if total_count > 0:
                average_ratio = total_ratio / total_count
                averages.append((average_ratio, pickup_location))

        # Sort by average ratio in descending order
        sorted_averages = sorted(averages, reverse=True)
        for average_ratio, pickup_location in sorted_averages:
            yield pickup_location, average_ratio


if __name__ == '__main__':
    AverageTipsToRevenueRatio.run()