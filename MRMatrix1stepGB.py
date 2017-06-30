import mrjob
from mrjob.job import MRJob
import ntpath


class MRMatrix1step(MRJob):
    
    
    matrix1name = "matrix1.txt"
    matrix2name = "matrix2.txt"
    rn = 300
    g = 4
    block = int(rn/g)
           
    def mapper(self, _, line):
        
        rown, coln, v  = map(int, line.strip().split())
        
        filename = mrjob.compat.jobconf_from_env('map.input.file')
        filename = ntpath.basename(filename)
        
        if filename == self.matrix1name:
            
            for i in range(self.g):
                yield (int(rown/self.block), i), (1, rown, coln, v)
            
        elif filename == self.matrix2name:
            
            for j in range(self.g):
                yield (j, int(coln/self.block)), (2, rown, coln, v)
            

    def reducer(self, k, values):
        import numpy as np
        
        (i, j) = k
        
        values_from1 = [[0 for p in range(self.rn)] for p in range(self.block)]
        values_from2 = [[0 for p in range(self.block)] for p in range(self.rn)]
        
        for (m, r, c, v) in values:
            if m == 1:
                values_from1[r-i*self.block][c] = v
            elif m == 2:
                values_from2[r][c-j*self.block] = v
        
        square = list(np.array(values_from1).dot(np.array(values_from2)))
        
        for l in range(self.block):
            for n in range(self.block):
                yield (l+i*self.block, n+j*self.block), square[l][n]
 
        

if __name__ == '__main__':
    #change the filename and matrix size here accordingly
    MRMatrix1step.matrix1name = "matrix1.txt"
    MRMatrix1step.matrix2name = "matrix2.txt"
    
    #matrix size and group can be adjusted
    MRMatrix1step.rn = 300
    MRMatrix1step.g = 50
    
    MRMatrix1step.block = int(MRMatrix1step.rn/MRMatrix1step.g)
    MRMatrix1step().run()
    
    
    
    