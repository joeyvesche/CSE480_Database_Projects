"""

Name: Joey Vesche
Time To Completion: 12 hours
Comments:

Sources:
https://www.tutorialspoint.com/How-to-zip-a-Python-Dictionary-and-List-together#:~:text=Use%20the%20zip()%20function,list%20as%20arguments%20to%20it.
https://docs.sqlalchemy.org/en/14/orm/queryguide.html
"""
import string
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
            columns = []
            if tokens[0] == "(":
                pop_and_check(tokens, "(")
                while True:
                    column_name = tokens.pop(0)
                    columns.append(column_name)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ","
            pop_and_check(tokens, "VALUES")
            row_values = []
            while True:
                pop_and_check(tokens, "(")
                row_contents = []
                while True:
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ","
                row_values.append(row_contents)
                if not tokens or tokens[0] != ",":
                    break
                pop_and_check(tokens, ",")
            self.database.insert_into(table_name, columns, row_values)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            distinct = False
            if tokens[0].upper() == "DISTINCT":
                distinct = True
                pop_and_check(tokens, "DISTINCT")
            output_columns = []
            where_clause = None
            while True:
                col = tokens.pop(0)
                output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table1_name = tokens.pop(0)
            if tokens[0].upper() == "LEFT":
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                table2_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                left_column_name = tokens.pop(0)
                pop_and_check(tokens, "=")
                right_column_name = tokens.pop(0)
                join_type = "LEFT OUTER JOIN"
                where_clause = "=".join([table1_name + "." + left_column_name, table2_name + "." + right_column_name])
            else:
                table2_name = None
                join_type = None
                where_clause = None

            if tokens and tokens[0].upper() == "WHERE":
                where_clause = parse_where_clause(tokens)

            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                col = tokens.pop(0)
                order_by_columns.append(col)
                if not tokens:
                    break
                pop_and_check(tokens, ",")

            return self.database.select(output_columns, table1_name, order_by_columns, where_clause, distinct,
                                        join_type, table2_name)

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            where_clause = parse_where_clause(tokens) if tokens and tokens[0].upper() == "WHERE" else None
            self.database.delete(table_name, where_clause)

        def update(tokens):
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            set_columns = {}
            while True:
                column_name = tokens.pop(0)
                pop_and_check(tokens, "=")
                value = tokens.pop(0)
                set_columns[column_name] = value
                if not tokens or tokens[0].upper() == "WHERE":
                    break
                if not tokens:
                    break
                pop_and_check(tokens, ",")
            where_clause = parse_where_clause(tokens) if tokens and tokens[0].upper() == "WHERE" else None
            self.database.update(table_name, set_columns, where_clause)

        def parse_where_clause(tokens):
            pop_and_check(tokens, "WHERE")
            column_name = tokens.pop(0)
            operator = tokens.pop(0)
            if operator == 'IS':
                if tokens[0] == 'NOT':
                    operator += ' NOT'
                    tokens.pop(0)
                value = tokens.pop(0)
            else:
                value = tokens.pop(0)
            return (column_name, operator, value)

        def pop_and_check(tokens, expected):
            actual = tokens.pop(0)
            assert actual == expected, f"Expected {expected}, but got {actual}"

        tokens = tokenize(statement)

        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "SET", "DISTINCT"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0].upper() == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0].upper() == "INSERT":
            insert(tokens)
            return []
        elif tokens[0].upper() == "SELECT":
            return select(tokens)
        elif tokens[0].upper() == "UPDATE":
            update(tokens)
        elif tokens[0].upper() == "DELETE":
            delete(tokens)

        assert not tokens

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

    def insert_into(self, table_name, columns, row_values):
        table = self.tables[table_name]
        if columns:
            assert len(columns) == len(row_values[0])
            column_indexes = [table.column_names.index(column) for column in columns]
            for row in row_values:
                assert len(row) == len(columns)
                values_to_insert = [None] * len(table.column_names)
                for i, value in zip(column_indexes, row):
                    values_to_insert[i] = value
                table.insert_new_row(values_to_insert)
        else:
            for row in row_values:
                assert len(row) == len(table.column_names)
                table.insert_new_row(row)

    def select(self, output_columns, table1_name, order_by_columns=None, where_clause=None, distinct=None,
               join_type=None, table2_name=None):
        assert table1_name in self.tables
        table1 = self.tables[table1_name]
        if join_type:
            if join_type == "LEFT OUTER JOIN":
                assert table2_name in self.tables
                table2 = self.tables[table2_name]
                result = table1.left_outer_join(table2, where_clause)
                where_clause = None
        else:
            result = table1

        return result.select_rows(output_columns, order_by_columns, where_clause, distinct)

    def delete(self, table_name, where_clause=None):
        table = self.tables[table_name]
        table.delete(where_clause)

    def update(self, table_name, set_values, where_clause=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        rows = table.select_rows(["*"], order_by_columns=[], where_clause=where_clause)
        for row in rows:
            row_dict = dict(zip(table.column_names, row))
            if table.check_where_clause(row_dict, where_clause):
                table.update_row(row_dict, set_values)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def update_row(self, row_dict, set_values):
        row_index = self.rows.index(row_dict)

        # Update the row_dict with the new values
        for col, value in set_values.items():
            row_dict[col] = value

        # Replace the old row with the updated row_dict
        self.rows[row_index] = row_dict

    def left_outer_join(self, right_table, on):
        joined_rows = []
        for left_row in self.rows:
            match_found = False
            for right_row in right_table.rows:
                if self.on_clause(left_row, right_row):
                    joined_row = {**left_row, **right_row}
                    joined_rows.append(joined_row)
                    match_found = True
            if not match_found:
                joined_row = {**left_row, **{col: None for col in right_table.column_names}}
                joined_rows.append(joined_row)
        for right_row in right_table.rows:
            match_found = False
            for joined_row in joined_rows:
                if self.on_clause(left_row, right_row):
                    match_found = True
                    break

        joined_table = Table(f"{self.name}_joined_{right_table.name}", list(
            zip(self.column_names + right_table.column_names, self.column_types + right_table.column_types)))
        for row in joined_rows:
            joined_table.insert_new_row([row[col] for col in joined_table.column_names])

        return joined_table

    def on_clause(self, left_row, right_row):
        return left_row['id'] == right_row['id']

    def delete(self, where_clause=None):
        if where_clause is None:
            self.rows = []
        else:
            column_name, operator, value = where_clause
            assert operator in {'=', '<', '>', '>=', '<=', '!='}, f"Unsupported operator: {operator}"
            self.rows = [row for row in self.rows if row[column_name] != value]

    def check_where_clause(self, row_dict, where_clause):
        if where_clause is None:
            return True
        else:
            column_name, operator, value = where_clause
            assert operator in {'=', '<', '>', '>=', '<=', '!='}, f"Unsupported operator: {operator}"
            if operator == '=':
                return row_dict[column_name] == value
            elif operator == '<':
                return row_dict[column_name] < value
            elif operator == '>':
                return row_dict[column_name] > value
            elif operator == '<=':
                return row_dict[column_name] <= value
            elif operator == '>=':
                return row_dict[column_name] >= value
            elif operator == '!=':
                return row_dict[column_name] != value

    def select_rows(self, output_columns, order_by_columns, where_clause=None, distinct=None):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(rows, order_by_columns):
            if order_by_columns == []:
                return rows

            def get_value(row, col):
                value = row[col]
                if value is None:
                    return (not reverse, None)
                return (reverse, value)

            for i, col in reversed(list(enumerate(order_by_columns))):
                reverse = False
                if col.endswith(" DESC"):
                    col = col[:-5]
                    reverse = True
                elif col.endswith(" ASC"):
                    col = col[:-4]

                rows = sorted(rows, key=lambda row: get_value(row, col))
            return rows

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        rows = self.rows
        if where_clause:

            column_name, operator, value = where_clause
            assert operator in {'=', '<', '>', '>=', '<=', '!=', 'IS', 'IS NOT'}, f"Unsupported operator: {operator}"
            if operator == '=':
                rows = [row for row in rows if row.get(column_name) == value]
            elif operator == '<':
                rows = [row for row in rows if row.get(column_name) is not None and row.get(column_name) < value]
            elif operator == '>':
                rows = [row for row in rows if row.get(column_name) is not None and row.get(column_name) > value]
            elif operator == '<=':
                rows = [row for row in rows if row.get(column_name) is not None and row.get(column_name) <= value]
            elif operator == '>=':
                rows = [row for row in rows if row.get(column_name) is not None and row.get(column_name) >= value]
            elif operator == '!=':
                rows = [row for row in rows if row.get(column_name) != value]
            elif operator == 'IS':
                rows = [row for row in rows if row.get(column_name) is value]
            elif operator == 'IS NOT':
                rows = [row for row in rows if row.get(column_name) is not value]
        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(rows, order_by_columns)
        if distinct:
            unique_rows = []
            unique_keys = set()
            for row in sorted_rows:
                key = tuple(row[col] for col in expanded_output_columns)
                if key not in unique_keys:
                    unique_rows.append(row)
                    unique_keys.add(key)
            sorted_rows = unique_rows
        return generate_tuples(sorted_rows, expanded_output_columns)



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
    text = ""
    while query:
        if query[0] == "'":
            if len(query) > 1 and query[1] == "'":
                # escaped quote
                text += "'"
                query = query[2:]
            else:
                # end of string
                tokens.append(text)
                query = query[1:]
                return query
        else:
            text += query[0]
            query = query[1:]
    raise AssertionError("Unterminated string")


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
        #print("Query:{}".format(query))
        #print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query.startswith('IS NOT'):
            tokens.append('IS NOT')
            query = query[6:]
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

        if query.startswith('!='):
            tokens.append('!=')
            query = query[2:]
            continue

        if query.startswith('.'):
            tokens.pop()
            query = query[1:]
            continue

        if query[0] in "><=!":
            tokens.append(query[0])
            query = query[1:]
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens