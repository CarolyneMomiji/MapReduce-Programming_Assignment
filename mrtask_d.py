#d.What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class AverageTripTime(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_trip_time, reducer=self.reducer_sum_trip_time),
            MRStep(reducer=self.reducer_calculate_average_trip_time)
        ]

    def mapper_get_trip_time(self, _, line):
        """Mapper to extract pickup location and trip time."""
        if not line.startswith('VendorID'):  # Skip header line
            fields = line.split(',')
            try:
                pickup_location = fields[7].strip()  # PULocationID (assumed to be the 8th field)
                pickup_time = datetime.strptime(fields[1].strip(), '%Y-%m-%d %H:%M:%S')  # Pickup time (2nd field)
                dropoff_time = datetime.strptime(fields[2].strip(), '%Y-%m-%d %H:%M:%S')  # Dropoff time (3rd field)
                trip_time = (dropoff_time - pickup_time).total_seconds() / 60  # Calculate trip time in minutes
                yield pickup_location, (trip_time, 1)  # Emit (pickup_location, (trip_time, 1))
            except (ValueError, IndexError):
                pass  # Skip lines with invalid data

    def reducer_sum_trip_time(self, pickup_location, trip_time_counts):
        """Reducer to sum trip times and counts for each pickup location."""
        total_trip_time = 0
        total_trips = 0
        for trip_time, count in trip_time_counts:
            total_trip_time += trip_time
            total_trips += count
        yield pickup_location, (total_trip_time, total_trips)  # Emit (pickup_location, (total_trip_time, total_trips))

    def reducer_calculate_average_trip_time(self, pickup_location, total_trip_time_and_counts):
        """Reducer to calculate average trip time for each pickup location."""
        total_trip_time = 0
        total_trips = 0
        for trip_time, trips in total_trip_time_and_counts:
            total_trip_time += trip_time
            total_trips += trips
        if total_trips > 0:
            average_trip_time = total_trip_time / total_trips
            yield pickup_location, average_trip_time  # Emit (pickup_location, average_trip_time)

if __name__ == '__main__':
    AverageTripTime.run()