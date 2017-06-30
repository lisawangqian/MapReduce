import mrjob
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
import ntpath



class MRMatrix2step(MRJob):
    
    matrix1name = "matrix1.txt"
    matrix2name = "matrix2.txt"
    rn = 300
    g = 4
    block = int(rn/g)
    
    #def configure_options(self):
        #super(MRMatrix2step, self).configure_options()
        #self.add_passthrough_option('--matrix1', default = self.matrix1name)
        #self.add_passthrough_option('--matrix2', default = self.matrix2name)
        

    def mapper_step1(self, _, line):
        rown, coln, v  = map(int, line.strip().split())
       
        filename = mrjob.compat.jobconf_from_env('map.input.file')
        filename = ntpath.basename(filename)
        
        if filename == self.matrix1name:
            for k in range(self.block):
                yield (int(rown/self.block), int(coln/self.block), k), (1, rown, coln, v)
            
        elif filename == self.matrix2name:
            for i in range(self.block):
                yield (i, int(rown/self.block), int(coln/self.block)), (2, rown, coln, v)
            

    def reducer_step1(self, key, values):
        import numpy as np
        
        (ig,jg,kg) = key
        
        values_from1 = [[0 for i in range(self.block)] for i in range(self.block)]
        values_from2 = [[0 for i in range(self.block)] for i in range(self.block)]
        
        for (m, r, c, v) in values:
            if m == 1:
                values_from1[r-ig*self.block][c-jg*self.block] = v
            elif m == 2:
                values_from2[r-jg*self.block][c-kg*self.block] = v
        
        square = list(np.array(values_from1).dot(np.array(values_from2)))       
        
        for l in range(self.block):
            for n in range(self.block):
                if (l+ig*self.block < self.rn) & (n+kg*self.block < self.rn):
                    yield (l+ig*self.block, n+kg*self.block), square[l][n]

    def mapper_step2(self, k, v):
        yield k, v

    def reducer_step2(self, k, values):
        yield k, sum(values)


    def steps(self):
        return [MRStep(mapper=self.mapper_step1,
                        reducer=self.reducer_step1),
                MRStep(mapper=self.mapper_step2,
                        reducer=self.reducer_step2)]
        

if __name__ == '__main__':
    #change the filename and matrix size here accordingly
    MRMatrix2step.matrix1name = "matrix1.txt"
    MRMatrix2step.matrix2name = "matrix2.txt"
    
    #matrix size and group can be adjusted
    MRMatrix2step.rn = 300
    MRMatrix2step.g = 50
    
    MRMatrix2step.block = int(MRMatrix2step.rn/MRMatrix2step.g)
    MRMatrix2step.run()