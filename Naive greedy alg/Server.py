class Server():
    def __init__(self, id):
        self.id = id

    def __eq__(self, other):
        return self.id == other.id
