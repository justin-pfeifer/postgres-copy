

class IterTest:
    def __init__(self, count=0):
        self.count = count
        self.n = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.n >= self.count:
            raise StopIteration
        self.n += 1
        return ','.join(str(x) for x in {'test': self.n}.values())
