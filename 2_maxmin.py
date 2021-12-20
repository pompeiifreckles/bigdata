from mrjob.job import MRJob

'''Sample Data
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,
ITE00100554,18000102,TMAX,-60,,I,E,
ITE00100554,18000102,TMIN,-125,,,E,
GM000010962,18000102,PRCP,0,,,E,
EZE00100082,18000102,TMAX,-44,,,E, 

Output I am expecting to see:
ITE00100554  32.3  20.2
EZE00100082  34.4  19.6
'''

class MaxMinTemperature(MRJob):
    def mapper(self, _, line):
        location, datetime, measure, temperature, w, x, y, z = line.split(',')
        temperature = float(temperature)/10
        if measure in ('TMAX', 'TMIN'):
            yield location, temperature

    def reducer(self, location, temperatures):
        min_temp = next(temperatures)
        max_temp = min_temp
        for item in temperatures:
            min_temp = min(item, min_temp)
            max_temp = max(item, max_temp)
        yield location, (min_temp, max_temp)


if __name__ == '__main__':
    MaxMinTemperature.run()