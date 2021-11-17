import sys

if sys.version_info[0] == 3:
    from functools import reduce
else:
    pass


def min(l):
    return reduce(lambda x, y: x if x < y else y, l, sys.maxsize)


def max(l):
    return reduce(lambda x, y: x if x > y else y, l, -sys.maxsize - 1)


if __name__ == "__main__":
    assert min([32, 63, 7, 10, 100]) == 7
    assert max([32, 63, 7, 10, 100]) == 100