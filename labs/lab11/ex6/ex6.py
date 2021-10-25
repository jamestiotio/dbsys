class Tree:
    def __init__(self,value,left=None,right=None):
        self.value = value
        self.left = left
        self.right = right
    def __str__(self):
        return "{value:%s, left:%s, right:%s}" % (str(self.value), str(self.left), str(self.right))
    
mytree = Tree(17, Tree(11, Tree(4), Tree(13)), Tree(5, None, Tree(30)))

# TODO: define fmap(f,t)

# TODO: define freduce(f,t,acc)
