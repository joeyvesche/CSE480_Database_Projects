"""

Name: Joey Vesche
Time To Completion: 3 hours
Comments:

Sources:
My brain! (you better cite your sources better than this)
"""
import string
import re
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")
            row_contents = []
            while True:
                item = tokens.pop(0)
                row_contents.append(item)
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.insert_into(table_name, row_contents)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            output_columns = []
            while True:
                col = tokens.pop(0)
                if col.startswith("'"):
                    while not col.endswith("'"):
                        col += ' ' + tokens.pop(0)
                    col = col.replace("''", "'")[1:-1]
                output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table_name = tokens.pop(0)
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                col = tokens.pop(0)
                order_by_columns.append(col)
                if not tokens:
                    break
                pop_and_check(tokens, ",")
            return self.database.select(
                output_columns, table_name, order_by_columns)

    def order_rows(self, rows, order_by_columns):
        return sorted(rows, key=itemgetter(*order_by_columns))

    def left_outer_join(self, other_table, join_column):
        """
        Performs a left outer join between this table and another table
        on the specified join_column.
        """
        assert join_column in self.column_names and join_column in other_table.column_names
        joined_table_name = self.name + "_joined_" + other_table.name
        join_column_index_self = self.column_names.index(join_column)
        join_column_index_other = other_table.column_names.index(join_column)
        joined_column_name_type_pairs = list(zip(
            self.column_names, self.column_types)) + list(zip(
            other_table.column_names, other_table.column_types))
        joined_column_name_type_pairs = list(
            dict.fromkeys(joined_column_name_type_pairs))
        self.database.create_new_table(joined_table_name, joined_column_name_type_pairs)
        joined_table = self.database.tables[joined_table_name]

        for row_self in self.rows:
            found_match = False
            for row_other in other_table.rows:
                if row_self[join_column] == row_other[join_column]:
                    joined_row = {**row_self, **row_other}
                    joined_table.insert_new_row(list(joined_row.values()))
                    found_match = True
            if not found_match:
                joined_row = row_self.copy()
                for col in other_table.column_names:
                    joined_row[col] = None
                joined_table.insert_new_row(list(joined_row.values()))

        return joined_table

    def where(self, rows, condition):
        """
        Filters a list of rows according to the provided condition.
        """
        filtered_rows = []
        for row in rows:
            if eval(condition, {}, row):
                filtered_rows.append(row)
        return filtered_rows

    def delete(self, condition=None):
        """
        Deletes rows from this table that satisfy the given condition.
        If no condition is given, deletes all rows from the table.
        """
        if condition is None:
            self.rows = []
        else:
            self.rows = self.where(self.rows, condition)

    def update(self, updates, condition=None):
        """
        Updates rows in this table according to the provided updates and
        condition. If no condition is given, updates all rows in the table.
        """
        for row in self.rows:
            if condition is None or eval(condition, {}, row):
                for column, value in updates.items():
                    row[column] = value

    def __len__(self):
        """
        Returns the number of rows in the table.
        """
        return len(self.rows)

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


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        output_columns = expand_star_column(output_columns)

        if order_by_columns:
            order_indices = [self.column_names.index(col) for col in order_by_columns]
            self.rows.sort(key=itemgetter(*order_indices))

        results = []
        for row in self.rows:
            result_row = tuple(row[col] for col in output_columns)
            results.append(result_row)

        return results

        def update_rows(self, table_name, set_clause, where_clause):
            """
            Changes the values of the rows in a table that meet the where_clause.
            """
            assert table_name in self.tables
            table = self.tables[table_name]
            set_pairs = set_clause.split(",")
            set_dict = {}
            for pair in set_pairs:
                col, val = pair.split("=")
                set_dict[col] = val
            rows_to_update = [row for row in table.rows if where_clause.evaluate(row)]
            for row in rows_to_update:
                for col, val in set_dict.items():
                    row[col] = val
            return []

        def delete_rows(self, table_name, where_clause):
            """
            Deletes the rows in a table that meet the where_clause.
            """
            assert table_name in self.tables
            table = self.tables[table_name]
            rows_to_delete = [row for row in table.rows if where_clause.evaluate(row)]
            for row in rows_to_delete:
                table.rows.remove(row)
            return []

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)

class WhereClause:
    def init(self, expression):
        self.expression = expression
    def evaluate(self, row):
        return eval(self.expression, {}, row)

class JoinClause:
    def init(self, table1, table2, on_clause):
        self.table1 = table1
        self.table2 = table2
        self.on_clause = on_clause

    def join(self):
        """
        Performs a left outer join on two tables.
        """
        joined_table = []
        for row1 in self.table1.rows:
            matched = False
            for row2 in self.table2.rows:
                if self.on_clause.evaluate(row1, row2):
                    matched = True
                    new_row = {**row1, **row2}
                    joined_table.append(new_row)
            if not matched:
                new_row = {**row1, **{col: None for col in self.table2.column_names}}
                joined_table.append(new_row)
        return joined_table

def remove_quote_escaping(s):
    return re.sub(r"(?<!')'(?!')", '', s)

def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens

def remove_single_quote_escape(string):
    """
    Removes the escaping of single quotes, and returns the string.
    """
    return string.replace("''", "'")


def qualify_column_names(table_name, column_names):
    qualified_columns = []
    for col in column_names:
        if "." in col:
            qualified_columns.append(col)
        else:
            qualified_columns.append(table_name + "." + col)
    return qualified_columns
