from multiprocessing import Pool

def do_something(i):
    print(i)
    return i*10

a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

if __name__ == '__main__':
    pool = Pool(processes=4)

    results = pool.map(do_something, a)

    pool.close()
    pool.join()