"""
Name: Joey Vesche
Netid: Veschejo
PID: A60985934
How long did this project take you?
13 hours

Sources:
https://www.guru99.com/python-regular-expressions-complete-tutorial.html

"""
import string
import re

_ALL_DATABASES = {}

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.filename = filename
        self.current_database = {}

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = tokenize(statement)

        if not tokens:
            return []

        if tokens[0] == "CREATE":
            if tokens[1] == "TABLE":
                self.create_table(tokens[2], tokens[4:-1])
                return []
        elif tokens[0] == "INSERT":
            if tokens[1] == "INTO":
                self.insert_into(tokens[2], tokens[4:])
                return []
        elif tokens[0] == "SELECT":
            for i in range(len(tokens)):
                if tokens[i] == "ORDER":
                    return self.select(tokens[1:])

    def create_table(self, table_name, columns):
        """
        Creates a table with the given name and columns.
        """
        self.current_database[table_name] = Table(table_name, columns)

    def insert_into(self, table_name, values):
        """
        Inserts a row into the given table with the given values.
        """
        self.current_database[table_name].insert(values)

    def select(self, tokens):
        """
        Executes a SELECT statement with the given tokens.
        """
        columns = []

        # Gets the FROM keyword
        for i in range(len(tokens)):
            if tokens[i] != "FROM":
                columns.append(tokens[i])

        table_name = tokens[tokens.index("FROM") + 1]

        # Makes sure there is ORDER
        if "ORDER" in tokens:
            order_by_index = tokens.index("ORDER")
        else:
            order_by_index = None

        if order_by_index:
            order_by = tokens[order_by_index + 2:]
        else:
            order_by = None
        # Executes the select/order by, which returns a list of tuples
        return self.current_database[table_name].select(columns, order_by)

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


def tokenize(query):
    tokens = []
    query = query.strip()

    while query:
        old_query = query

        # Remove leading whitespace
        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        # Find SQL keywords
        if query[0] in string.ascii_letters:
            keyword = re.match(r'\b\w+\b', query)
            tokens.append(keyword.group(0))
            query = query[len(keyword.group(0)):]
            continue

        # Check for ints and floats
        if query[0] in "0123456789.":
            number = re.match(r"^\d+(?:\.\d+)?", query)
            tokens.append(number.group(0))
            query = query[len(number.group(0)):]
            continue

        # Special Characters
        if query[0] in "(),;":
            tokens.append(query[0])
            query = query[1:]
            continue

        # Handle "'"
        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        # Handle '*'
        if query[0] == '*':
            tokens.append(query[0])
            query = query[1:]
            continue
        # Break when gone through all elements
        if len(query) == len(old_query):
            break
    return tokens


def remove_leading_whitespace(query, tokens):
    """
    Removes the whitespace from the string until we hit another
    type of character
    """
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def collect_characters(query, allowed_characters):
    """
    Return the beginning of the string until we hit a character that's
    not allowed
    """
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_word(query, tokens):
    """
    Same as whitespace, except that allowed characters and ascii letters/digits
    """
    word = collect_characters(query,
                              string.ascii_letters + " " + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    """
    Looks for the other end of a single quote
    """
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


class Database(object):
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns):
        self.tables[table_name] = Table(table_name, columns)

    def insert_into(self, table_name, values):
        self.tables[table_name].insert(values)

    def select_from(self, table_name, columns, order_by=None):
        table = self.tables[table_name]
        result = table.select(columns)
        if order_by:
            result = table.order_by(*order_by)
        return result


class Table(object):
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.rows = []

    def insert(self, values):
        for word in range(len(values)):
            if values[word] == "(" or values[word] == "," or values[word] == ")" or values[word] == ";":
                continue
            else:
                # Convert NULL to None
                if values[word] == "NULL":
                    values[word] = None
                # Convert string to float or an int
                elif isfloat(values[word]):
                    temp = False
                    for i in values[word]:
                        if i == '.':
                            temp = True
                    if temp:
                        values[word] = float(values[word])

                    else:
                        values[word] = int(values[word])

        self.rows.append(values)

    def select(self, *column_names):
        #Remove anything that in unimportant
        column_names = column_names[1]
        column_names.remove(';')
        c = column_names.count(',')
        for i in range(c):
            column_names.remove(',')

        order = []
        order_temp = []

        # Get things that are important and add to a list
        for i in range(len(self.rows)):
            temp = {}
            for col in range(0, len(self.columns)-1, 3):
                temp[self.columns[col]] = None
                order_temp.append(self.columns[col])

        # Remove anything unimportant
        for i in self.rows:
            c = i.count(',')
            i.remove('(')
            i.remove(')')
            i.remove(';')
            for j in range(c):
                i.remove(',')

        # Create a key, value pair for each inserted element
        for i in self.rows:
            res = {}
            for key in order_temp:
                for value in i:
                    res[key] = value
                    i.remove(value)
                    break
            order.append(res)

        # Sort the list of dictionaries by the ORDER BY
        if len(column_names) == 2:
            order = sorted(order, key=lambda i: (i[column_names[0]], i[column_names[1]]))
        else:
            order = sorted(order, key=lambda i: (i[column_names[0]]))

        # Convert this list of dictionaries to a list of tuples to return
        new_order = []
        for i in order:
            temp = []
            for key, value in i.items():
                temp.append(value)
            new_order.append(tuple(temp))
        return new_order

class Row(object):
    def __init__(self, table, values):
        self.table = table
        self.values = values


def isfloat(num):
    """
    Return bool telling you if the string is a float
    """
    try:
        float(num)
        return True
    except ValueError:
        return False
