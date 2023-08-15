import sqlite3
import sys


class Hat:
    def __init__(self, id, topping,supplier,quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
               INSERT INTO Hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, hat_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, topping, supplier, quantity FROM hats WHERE id = ?
        """, [hat_id])
        return Hat(*c.fetchone())


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
               INSERT INTO suppliers (id, name) VALUES (?, ?)
           """, [supplier.id, supplier.name])

    def find(self, supplier_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, name FROM suppliers WHERE id = ?
        """, [supplier_id])
        return Supplier(*c.fetchone())


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""
               INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
           """, [order.id, order.location, order.hat])

    def find(self, order_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, name FROM orders WHERE id = ?
        """, [order_id])
        return Supplier(*c.fetchone())

# The Repository
class _Repository:
    def __init__(self, db):
        self._conn = sqlite3.connect(db)
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        try:
            self._conn.execute("""DROP TABLE hats""")
        except:pass
        try:
            self._conn.execute("""DROP TABLE suppliers""")
        except:pass
        try:
            self._conn.execute("""DROP TABLE orders""")
        except:pass
        self._conn.executescript("""
        CREATE TABLE hats (
            id      INT         PRIMARY KEY,
            topping    TEXT        NOT NULL,
            supplier      INT         NOT NULL,
            quantity      INT         NOT NULL,
            
            FOREIGN KEY(supplier)     REFERENCES suppliers(supplier)
        );

        CREATE TABLE suppliers (
            id                 INT     PRIMARY KEY,
            name     TEXT    NOT NULL
        );

        CREATE TABLE orders (
            id      INT     PRIMARY KEY,
            location  TEXT     NOT NULL,
            hat           INT     NOT NULL,

            FOREIGN KEY(hat)     REFERENCES hats(hat)

        );
    """)


if __name__ == '__main__':
    repo = _Repository(sys.argv[4])
    repo.create_tables()
    configurationFile = open(sys.argv[1], 'r')
    hatsAnSuppliers = configurationFile.readline()
    numbersList = hatsAnSuppliers.split(',')
    topToIndex = [""]
    for i in range(int(numbersList[0])):
        hatLine = configurationFile.readline().split('\n')
        currentHat = hatLine[0].split(',')
        repo.hats.insert(Hat(int(currentHat[0]), currentHat[1], int(currentHat[2]), int(currentHat[3])))
        topToIndex.append(currentHat[1])
    for i in range(int(numbersList[1])):
        supplierLine = configurationFile.readline().split('\n')
        currentSupplier = supplierLine[0].split(',')
        repo.suppliers.insert(Supplier(int(currentSupplier[0]), currentSupplier[1]))
    ordersFile = open(sys.argv[2], 'r')
    outputFile = open(sys.argv[3], 'w')
    ordersCount=1
    for line in ordersFile:
        OrderLine = line.split('\n')
        currentOrder = OrderLine[0].split(',')

        index = -1
        for i in range(1, len(topToIndex)):
            if topToIndex[i] == currentOrder[1]:
                if index == -1:
                    index = i
                elif repo.hats.find(i).supplier < repo.hats.find(index).supplier:
                    index = i
        repo.orders.insert(Order(ordersCount, currentOrder[0], index))
        ordersCount = ordersCount + 1
        repo._conn.execute("""Update hats set quantity = quantity-1 where id = ?""", [index])
        outputFile.write(str(currentOrder[1]+','+repo.suppliers.find(repo.hats.find(index).supplier).name+','+currentOrder[0]+'\n'))
        if repo.hats.find(index).quantity == 0:
            topToIndex[index]=""
            repo._conn.execute("""DELETE FROM hats where id = ?""", [index])
    repo._conn.commit()
    outputFile.close()







