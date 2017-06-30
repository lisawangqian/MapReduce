from mrjob.job import MRJob
import mrjob
import os

class MRWordFrequencyCount(MRJob):
    def configure_options(self):
        super(MRWordFrequencyCount, self).configure_options()
        self.add_passthrough_option('--myfile1', type='string')
        self.add_passthrough_option('--myfile2', type='string')
        
           

    def mapper(self, _, line):
        fullfilename = mrjob.compat.jobconf_from_env('map.input.file')
        filename = fullfilename.split("/")[-1]
        if filename == "myfile1.txt":
            yield "chars", len(line)
            yield "words", len(line.split())
            yield "lines", 1
        elif filename == "myfile2.txt":
            yield "chars2", len(line)
            yield "words2", len(line.split())
            yield "lines2", 1 

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()