import sys

if sys.version_info[0] == 3:
    from functools import reduce
else:
    pass


class Tree:
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.left = left
        self.right = right

    def __str__(self):
        return "{value:%s, left:%s, right:%s}" % (
            str(self.value),
            str(self.left),
            str(self.right),
        )


mytree = Tree(17, Tree(11, Tree(4), Tree(13)), Tree(5, None, Tree(30)))

print(str(mytree))


def tmap(f, t):
    if t is None:
        return None
    return Tree(f(t.value), tmap(f, t.left), tmap(f, t.right))


print(str(tmap(lambda x: x + 1, mytree)))


def treduce(f, t, acc):
    if t is None:
        return acc
    # This only works if an identity accumulator exists and if we use it (0, in this case)
    # return f(f(treduce(f, t.left, acc), treduce(f, t.right, acc)), t.value)
    # This works for more general cases
    return treduce(f, t.right, (treduce(f, t.left, f(t.value, acc))))


print(treduce(lambda x, y: x + y, mytree, 0))

myllist = [
    ["one", "two", "two", "three", "three", "three"],
    ["four", "four", "four", "four", "five"],
    ["five", "five", "five", "five"],
]


def llmap(f, ll):
    return list(map(lambda l: list(map(f, l)), ll))


ll1 = llmap(lambda w: 1, myllist)

print(ll1)


def llreduce(f, ll, acc):
    # combined = map(lambda l: reduce(f, l, acc), ll)  # wrong, acc being aggregated multiple times
    combined = map(lambda l: reduce(f, l), ll)
    return reduce(f, combined, acc)


print(llreduce(lambda x, y: x + y, ll1, 0))
