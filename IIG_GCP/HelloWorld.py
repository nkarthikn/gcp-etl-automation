class parent(object):
    def __init__(self, message):
        self._test_var = [1,2,3]
        print "Hello world ", message

    @property
    def test_var(self):
        return self._test_var

    @test_var.setter
    def test_var(self, test_var):
        self._test_var = test_var


class child(parent):
    def __init__(self, message):
        super(child, self).__init__("from child")
        print "child world", message
        self._test_var = {"hello":"world"}


def testing(myinput) :
    print "Check this "
    print type(myinput)

if __name__ == '__main__':
    c = child("child")
    print "C", c.test_var
    testing(c.test_var)
    print isinstance(c, parent)