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
    return f(f(treduce(f, t.left, acc), treduce(f, t.right, acc)), t.value)


print(treduce(lambda x, y: x + y, mytree, 0))
