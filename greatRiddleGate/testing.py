from multiprocessing import Pool
import pandas

def say_hello(name='world'):
    print "Hello, %s" % name
    return pandas.DataFrame({'one' : [name]})


lis = ['Daniel','Susan','Joe']

pool = Pool(4)

results = pool.map(say_hello, lis)
#close the pool and wait for the work to finish
pool.close()
pool.join()


print "these are the results:"
print results[1]
