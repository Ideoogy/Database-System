import string
import copy
from operator import itemgetter
from collections import namedtuple
from functools import cmp_to_key
import itertools

_ALL_DATABASES = {}
_TRANSACTION_CNTR = 0

WhereClause = namedtuple("WhereClause", ["col_name", "operator", "constant"])
UpdateClause = namedtuple("UpdateClause", ["col_name", "constant"])
FromJoinClause = namedtuple("FromJoinClause", ["left_table_name",
                                               "right_table_name",
                                               "left_join_col_name",
                                               "right_join_col_name"])


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
    
        self.filename = filename
        self.begin_transaction = False
        self.temp_database = None
        self.needs_exclusive = False
        self.id_ = _TRANSACTION_CNTR
        self.view_queries = {}
        self.view_select = False
        self.collations = {}
        
    def create_collation(self, collation_name, function):
        self.collations[collation_name] = function
    
    def executemany(self, statement, value_sets):
        for value_set in value_sets:
            new_statement = statement
            for value in value_set:
                if(isinstance(value,str)):
                    replace_value = "\'" + value + "\'"
                else:
                    replace_value = str(value)
                new_statement = new_statement.replace("?",replace_value,1)
            self.execute(new_statement)
        
    def execute(self, statement):
        
        def begin(tokens):
            pop_and_check(tokens, "BEGIN")
            if(tokens[0] == "DEFERRED"):
                tokens.pop(0)
            elif(tokens[0] == "IMMEDIATE"):
                tokens.pop(0)
                self.database.add_reserved_lock(self.id_)
            elif(tokens[0] == "EXCLUSIVE"):
                tokens.pop(0)
                self.database.add_exclusive_lock(self.id_)
            pop_and_check(tokens, "TRANSACTION")
            assert self.begin_transaction == False
            self.begin_transaction = True
            self.temp_database = copy.deepcopy(self.database)
            
        def commit(tokens):
            pop_and_check(tokens, "COMMIT")
            pop_and_check(tokens, "TRANSACTION")
            assert self.begin_transaction == True
            self.database.commit(self.id_, self.needs_exclusive)
            self.begin_transaction = False
            self.database.tables = self.temp_database.tables
            self.temp_database = None
            self.needs_exclusive = False
            
        def create(tokens):
            pop_and_check(tokens, "CREATE")
            if tokens[0] == "TABLE":
                create_table(tokens)
            else:
                create_view(tokens)
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
            pop_and_check(tokens, "TABLE")
            notexists = False
            if(tokens[0] == "IF"):
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                notexists = True
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            default_values = {}
            while True:
                column_name = tokens.pop(0)
                qual_col_name = QualifiedColumnName(column_name, table_name)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((qual_col_name, column_type))
                if(tokens[0] == "DEFAULT"):
                    pop_and_check(tokens, "DEFAULT")
                    default_val = tokens.pop(0)
                    default_values[qual_col_name] = default_val
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs, notexists, default_values)

        def create_view(tokens):
            pop_and_check(tokens, "VIEW")
            view_name = tokens.pop(0)
            pop_and_check(tokens, "AS")
            self.view_queries[view_name] = tokens
            test_view = self.view_queries[view_name]
            
        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            def get_comma_seperated_contents(tokens):
                contents = []
                pop_and_check(tokens, "(")
                while True:
                    item = tokens.pop(0)
                    contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        return contents
                    assert comma_or_close == ',', comma_or_close

            pop_and_check(tokens, "INSERT")
            if(self.begin_transaction == True):
                self.needs_exclusive = True
                self.database.add_reserved_lock(self.id_)
            else:
                self.database.add_reserved_lock(self.id_)
                self.database.add_exclusive_lock(self.id_)
                self.database.release_reserved_lock(self.id_)
                self.database.release_exclusive_lock(self.id_)
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            if tokens[0] == "DEFAULT":
                pop_and_check(tokens, "DEFAULT")
                pop_and_check(tokens, "VALUES")
                if self.begin_transaction == True:
                    self.temp_database.insert_default_into(table_name)
                else:
                    self.database.insert_default_into(table_name)
            else:
                if tokens[0] == "(":
                    col_names = get_comma_seperated_contents(tokens)
                    qual_col_names = [QualifiedColumnName(col_name, table_name)
                                      for col_name in col_names]
                else:
                    qual_col_names = None
                pop_and_check(tokens, "VALUES")
                while tokens:
                    row_contents = get_comma_seperated_contents(tokens)
                    if qual_col_names:
                        assert len(row_contents) == len(qual_col_names)
                    if self.begin_transaction == True:
                        self.temp_database.insert_into(table_name,
                                              row_contents,
                                              qual_col_names=qual_col_names)
                    else:
                        self.database.insert_into(table_name,
                                                  row_contents,
                                                  qual_col_names=qual_col_names)
                    if tokens:
                        pop_and_check(tokens, ",")

        def get_qualified_column_name(tokens):
            """
            Returns comsumes tokens to  generate tuples to create
            a QualifiedColumnName.
            """
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)

        def update(tokens):
            pop_and_check(tokens, "UPDATE")
            if(self.begin_transaction == True):
                self.needs_exclusive = True
                self.database.add_reserved_lock(self.id_)
            else:
                self.database.add_reserved_lock(self.id_)
                self.database.add_exclusive_lock(self.id_)
                self.database.release_reserved_lock(self.id_)
                self.database.release_exclusive_lock(self.id_)
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            update_clauses = []
            while tokens:
                qual_name = get_qualified_column_name(tokens)
                if not qual_name.table_name:
                    qual_name.table_name = table_name
                pop_and_check(tokens, '=')
                constant = tokens.pop(0)
                update_clause = UpdateClause(qual_name, constant)
                update_clauses.append(update_clause)
                if tokens:
                    if tokens[0] == ',':
                        tokens.pop(0)
                        continue
                    elif tokens[0] == "WHERE":
                        break

            where_clause = get_where_clause(tokens, table_name)
            if self.begin_transaction == True:
                self.temp_database.update(table_name, update_clauses, where_clause)
            else:
                self.database.update(table_name, update_clauses, where_clause)

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            if(self.begin_transaction == True):
                self.needs_exclusive = True
                self.database.add_reserved_lock(self.id_)
            else:
                self.database.add_reserved_lock(self.id_)
                self.database.add_exclusive_lock(self.id_)
                self.database.release_reserved_lock(self.id_)
                self.database.release_exclusive_lock(self.id_)
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            where_clause = get_where_clause(tokens, table_name)
            if self.begin_transaction == True:
                self.temp_database.delete(table_name, where_clause)
            else:
                self.database.delete(table_name, where_clause)

        def get_where_clause(tokens, table_name):
            if not tokens or tokens[0] != "WHERE":
                return None
            tokens.pop(0)
            qual_col_name = get_qualified_column_name(tokens)
            if not qual_col_name.table_name:
                qual_col_name.table_name = table_name
            operators = {">", "<", "=", "!=", "IS"}
            found_operator = tokens.pop(0)
            assert found_operator in operators
            if tokens[0] == "NOT":
                tokens.pop(0)
                found_operator += " NOT"
            constant = tokens.pop(0)
            if constant is None:
                assert found_operator in {"IS", "IS NOT"}
            if found_operator in {"IS", "IS NOT"}:
                assert constant is None
            return WhereClause(qual_col_name, found_operator, constant)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            tokens_copy = copy.deepcopy(tokens)
            def get_from_join_clause(tokens):
                left_table_name = tokens.pop(0)
                if tokens[0] != "LEFT":
                    return FromJoinClause(left_table_name, None, None, None)
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                right_table_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                left_col_name = get_qualified_column_name(tokens)
                pop_and_check(tokens, "=")
                right_col_name = get_qualified_column_name(tokens)
                return FromJoinClause(left_table_name,
                                      right_table_name,
                                      left_col_name,
                                      right_col_name)
            pop_and_check(tokens, "SELECT")
            if(self.begin_transaction == True):
                self.database.add_shared_lock(self.id_)
            else:
                self.database.add_shared_lock(self.id_)
                self.database.remove_shared_lock(self.id_)

            is_distinct = tokens[0] == "DISTINCT"
            if is_distinct:
                tokens.pop(0)

            output_columns = []
            aggregates = []
            while True:
                if tokens[0] == "max" or tokens[0] == "min":
                    min_or_max = tokens.pop(0)
                    aggregates.append(min_or_max)
                    pop_and_check(tokens,"(")
                    qual_col_name = get_qualified_column_name(tokens)
                    output_columns.append(qual_col_name)
                    pop_and_check(tokens,")")
                else:
                    qual_col_name = get_qualified_column_name(tokens)
                    output_columns.append(qual_col_name)
                    aggregates.append(None)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            """
            Select From View
            """
            if tokens[0] in self.view_queries and self.view_select == False:
                view_name = tokens[0]
                for qual_col in output_columns:
                    qual_col.table_name = view_name
                view_query = copy.deepcopy(self.view_queries[view_name])
                view_query_tokens = copy.deepcopy(self.view_queries[view_name])
                view_query_result = list(select(view_query))
                if view_query_result == []:
                    return []
                else:
                    column_name_type_pairs = []
                    column_names = []
                    view_query_tokens.pop(0)
                    is_distinct = tokens[0] == "DISTINCT"
                    if is_distinct:
                        tokens.pop(0)
                    while True:
                        col = view_query_tokens.pop(0)
                        if view_query_tokens[0] == '.':
                            view_query_tokens.pop(0)
                            col = view_query_tokens.pop(0)
                        column_names.append(col)
                        comma_or_from = view_query_tokens.pop(0)
                        if comma_or_from == "FROM":
                                break
                        assert comma_or_from == ','
                    table_name = view_query_tokens.pop(0)
                    if column_names[0] == "*":
                        column_names = self.database.tables[table_name].column_names
                    row_1 = view_query_result[0]
                    for i in range(len(row_1)):
                        if isinstance(row_1[i], str):
                            column_type = "TEXT"
                        else:
                            column_type = "INTEGER"
                        if isinstance(column_names[i], QualifiedColumnName):
                            column_name_type_pairs.append((QualifiedColumnName(column_names[i].col_name, view_name),column_type))
                        else:
                            column_name_type_pairs.append((QualifiedColumnName(column_names[i], view_name),column_type))
                    self.database.create_new_table(view_name, column_name_type_pairs, False, {})
                    for view_query_val in view_query_result:
                        self.database.insert_into(view_name, list(view_query_val))
                    self.view_select = True
                    result = select(tokens_copy)
                    self.database.delete_table(view_name)
                    return result
            else:
                # FROM or JOIN

                from_join_clause = get_from_join_clause(tokens)
                table_name = from_join_clause.left_table_name
                # WHERE
                where_clause = get_where_clause(tokens, table_name)
    
                # ORDER BY
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                order_by_columns = []
                if self.view_select == True:
                    for output_col in output_columns:
                        output_col.table_name = table_name
                    while True:
                        col_name = tokens.pop(0)
                        qual_col_name = QualifiedColumnName(col_name,table_name)
                        collate = None
                        descend = False
                        if not tokens:
                            order_by_columns.append((qual_col_name,collate,descend))
                            break
                        if tokens[0] == "COLLATE":
                            pop_and_check(tokens,"COLLATE")
                            collate_name = tokens.pop(0)
                            collate = self.collations[collate_name]
                        if not tokens:
                            order_by_columns.append((qual_col_name,collate,descend))
                            break
                        if tokens[0] == "DESC":
                            pop_and_check(tokens,"DESC")
                            descend = True
                        order_by_columns.append((qual_col_name,collate,descend))
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                    self.view_select = False
                else:
                    while True:
                        qual_col_name = get_qualified_column_name(tokens)
                        collate = None
                        descend = False
                        if not tokens:
                            order_by_columns.append((qual_col_name,collate,descend))
                            break
                        if tokens[0] == "COLLATE":
                            pop_and_check(tokens,"COLLATE")
                            collate_name = tokens.pop(0)
                            collate = self.collations[collate_name]
                        if not tokens:
                            order_by_columns.append((qual_col_name,collate,descend))
                            break
                        if tokens[0] == "DESC":
                            pop_and_check(tokens,"DESC")
                            descend = True
                        order_by_columns.append((qual_col_name,collate,descend))
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                if self.begin_transaction == True:
                    result = self.temp_database.select(
                    output_columns,
                    order_by_columns,
                    from_join_clause=from_join_clause,
                    where_clause=where_clause,
                    is_distinct=is_distinct)
                else:
                    result = self.database.select(
                    output_columns,
                    order_by_columns,
                    from_join_clause=from_join_clause,
                    where_clause=where_clause,
                    is_distinct=is_distinct)
                if "min" in aggregates or "max" in aggregates:
                    result_list = list(result)
                    aggregated_list = []
                    for i in range(len(aggregates)):
                        aggr = aggregates[i]
                        if aggr == "min":
                            min_val = result_list[0][i]
                            for row in result_list:
                                if row[i] < min_val:
                                    min_val = row[i]
                            check_for_val = min_val
                        else:
                            max_val = result_list[0][i]
                            for row in result_list:
                                if row[i] > max_val:
                                    max_val = row[i]
                            check_for_val = max_val
                        aggregated_list.append(check_for_val)
                    return [tuple(aggregated_list)]
                else:
                    return result
                
        def drop(tokens):
            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")
            ifexists = False
            if(tokens[0] == "IF"):
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "EXISTS")
                ifexists = True
            table_name = tokens.pop(0)
            if(self.begin_transaction == True):
                self.temp_database.drop(table_name, ifexists)
            else:
                self.database.drop(table_name, ifexists)
                
        def rollback(tokens):
            assert self.begin_transaction == True
            pop_and_check(tokens, "ROLLBACK")
            pop_and_check(tokens, "TRANSACTION")
            self.database.release_exclusive_lock(self.id_)
            self.database.release_reserved_lock(self.id_)
            self.database.remove_shared_lock(self.id_)
            self.begin_transaction = False
            self.needs_exclusive = False
            self.temp_database = None
            
        self.database = _ALL_DATABASES[self.filename]

        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "DROP":
            drop(tokens)
            return []
        elif tokens[0] == "ROLLBACK":
            rollback(tokens)
            return []
        elif tokens[0] == "BEGIN":
            begin(tokens)
            return []
        elif tokens[0] == "COMMIT":
            commit(tokens)
            return []
        else:
            raise AssertionError(
                "Unexpected first word in statements: " + tokens[0])

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    global _TRANSACTION_CNTR
    _TRANSACTION_CNTR = _TRANSACTION_CNTR + 1
    return Connection(filename)


class QualifiedColumnName:

    def __init__(self, col_name, table_name=None):
        self.col_name = col_name
        self.table_name = table_name

    def __str__(self):
        return "QualifiedName({}.{})".format(
            self.table_name, self.col_name)

    def __eq__(self, other):
        same_col = self.col_name == other.col_name
        if not same_col:
            return False
        both_have_tables = (self.table_name is not None and
                            other.col_name is not None)
        if not both_have_tables:
            return True
        return self.table_name == other.table_name

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.col_name, self.table_name))

    def __repr__(self):
        return str(self)


class Database:

    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.shared_locks = set([])
        self.exclusive_lock = None
        self.reserved_lock = None
        
    def add_reserved_lock(self, id_):
        if self.reserved_lock == None and self.exclusive_lock == None:
            self.reserved_lock = id_
        elif self.reserved_lock == id_:
            pass
        elif self.exclusive_lock == id_:
            pass
        else:
            raise Exception()
        
    def release_reserved_lock(self, id_):
        if(self.reserved_lock == id_):
            self.reserved_lock = None
    
    def add_shared_lock(self, id_):
        if self.exclusive_lock == id_:
            pass
        elif self.exclusive_lock != None:
            raise Exception()
        self.shared_locks.add(id_)
    
    def remove_shared_lock(self, id_):
        if id_ in self.shared_locks:
            self.shared_locks.remove(id_)
        
    def release_exclusive_lock(self, id_):
        if(self.exclusive_lock == id_):
            self.exclusive_lock = None
        
    def add_exclusive_lock(self, id_):
        if self.exclusive_lock != None and self.exclusive_lock != id_:
            raise Exception()
        if self.reserved_lock != None and self.reserved_lock != id_:
            raise Exception()
        if len(self.shared_locks) == 0 or (len(self.shared_locks) == 1 and id_ in self.shared_locks):
            self.exclusive_lock = id_
        else:
            raise Exception()
        
    def commit(self, id_, needs_exclusive):
        if needs_exclusive:
            self.add_exclusive_lock(id_)
        self.release_exclusive_lock(id_)
        self.remove_shared_lock(id_)
        self.release_reserved_lock(id_)

    def create_new_table(self, table_name, column_name_type_pairs, notexists, default_values):
        if notexists:
            if(table_name not in self.tables):
                self.tables[table_name] = Table(table_name, column_name_type_pairs)
                self.tables[table_name].set_default(default_values)
        else:
            assert table_name not in self.tables
            self.tables[table_name] = Table(table_name, column_name_type_pairs)
            self.tables[table_name].set_default(default_values)
        return []
        
    def delete_table(self, table_name):
        del self.tables[table_name]

    def insert_into(self, table_name, row_contents, qual_col_names=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, qual_col_names=qual_col_names)
        return []
        
    def insert_default_into(self, table_name):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_default_row()
        return []

    def update(self, table_name, update_clauses, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(update_clauses, where_clause)

    def delete(self, table_name, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete(where_clause)
        
    def drop(self, table_name, ifexists):
        if ifexists:
            if table_name in self.tables:
                del self.tables[table_name]
        else:
            assert table_name in self.tables
            del self.tables[table_name]

    def select(self, output_columns, order_by_columns,
               from_join_clause,
               where_clause=None, is_distinct=False):
        assert from_join_clause.left_table_name in self.tables
        if from_join_clause.right_table_name:
            assert from_join_clause.right_table_name in self.tables
            left_table = self.tables[from_join_clause.left_table_name]
            right_table = self.tables[from_join_clause.right_table_name]
            all_columns = itertools.chain(
                zip(left_table.column_names, left_table.column_types),
                zip(right_table.column_names, right_table.column_types))
            left_col = from_join_clause.left_join_col_name
            right_col = from_join_clause.right_join_col_name
            join_table = Table("", all_columns)
            combined_rows = []
            for left_row in left_table.rows:
                left_value = left_row[left_col]
                found_match = False
                for right_row in right_table.rows:
                    right_value = right_row[right_col]
                    if left_value is None:
                        break
                    if right_value is None:
                        continue
                    if left_row[left_col] == right_row[right_col]:
                        new_row = dict(left_row)
                        new_row.update(right_row)
                        combined_rows.append(new_row)
                        found_match = True
                        continue
                if left_value is None or not found_match:
                    new_row = dict(left_row)
                    new_row.update(zip(right_row.keys(),
                                       itertools.repeat(None)))
                    combined_rows.append(new_row)

            join_table.rows = combined_rows
            table = join_table
        else:
            table = self.tables[from_join_clause.left_table_name]
        return table.select_rows(output_columns, order_by_columns,
                                 where_clause=where_clause,
                                 is_distinct=is_distinct)


class Table:

    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_name_type_pairs = column_name_type_pairs
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []
        self.default_values = {}

    def insert_new_row(self, row_contents, qual_col_names=None):
        if not qual_col_names:
            qual_col_names = self.column_names
        assert len(qual_col_names) == len(row_contents)
        row = dict(zip(qual_col_names, row_contents))
        for null_default_col in set(self.column_names) - set(qual_col_names):
            if null_default_col in self.default_values:
                row[null_default_col] = self.default_values[null_default_col]
            else:
                row[null_default_col] = None
        self.rows.append(row)
        
    def insert_new_default_row(self):
        qual_col_names = self.column_names
        row_contents = []
        for col_name in qual_col_names:
            row_contents.append(self.default_values[col_name])
        row = dict(zip(qual_col_names, row_contents))
        self.rows.append(row)

    def update(self, update_clauses, where_clause):
        for row in self.rows:
            if self._row_match_where(row, where_clause):
                for update_clause in update_clauses:
                    row[update_clause.col_name] = update_clause.constant

    def delete(self, where_clause):
        self.rows = [row for row in self.rows
                     if not self._row_match_where(row, where_clause)]
                     
    def set_default(self, default_values):
        self.default_values = default_values

    def _row_match_where(self, row, where_clause):
        if not where_clause:
            return True
        new_rows = []
        value = row[where_clause.col_name]

        op = where_clause.operator
        cons = where_clause.constant
        if ((op == "IS NOT" and (value is not cons)) or
                (op == "IS" and value is cons)):
            return True

        if value is None:
            return False

        if ((op == ">" and value > cons) or
            (op == "<" and value < cons) or
            (op == "=" and value == cons) or
                (op == "!=" and value != cons)):
            return True
        return False

    def select_rows(self, output_columns, order_by_columns,
                    where_clause=None, is_distinct=False):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col.col_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names
                       for col in columns)

        def ensure_fully_qualified(columns):
            for col in columns:
                if col.table_name is None:
                    col.table_name = self.name

        def sort_rows(rows, order_by_columns):
            order_by_columns.reverse()
            for order_by in order_by_columns:
                column = order_by[0]
                collate = order_by[1]
                descend = order_by[2]
                if collate != None:
                    """
                    Hints taken from https://piazza.com/class/jqh746nkrro51r?cid=701
                    https://python-reference.readthedocs.io/en/latest/docs/functions/sorted.html
                    https://stackoverflow.com/questions/34981262/porting-sort-with-lambda-function-to-python-3
                    """
                    rows = sorted(rows, key=cmp_to_key(lambda x, y: collate(x[column],y[column])), reverse = descend)
                else:
                    rows = sorted(rows, key=itemgetter(column), reverse = descend)
            return rows

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        def remove_duplicates(tuples):
            seen = set()
            uniques = []
            for row in tuples:
                if row in seen:
                    continue
                seen.add(row)
                uniques.append(row)
            return uniques

        expanded_output_columns = expand_star_column(output_columns)

        check_columns_exist(expanded_output_columns)
        ensure_fully_qualified(expanded_output_columns)
        order_columns = []
        for order_by_column in order_by_columns:
            order_columns.append(order_by_column[0])
        check_columns_exist(order_columns)
        ensure_fully_qualified(order_columns)

        filtered_rows = [row for row in self.rows
                         if self._row_match_where(row, where_clause)]
        sorted_rows = sort_rows(filtered_rows, order_by_columns)

        list_of_tuples = generate_tuples(sorted_rows, expanded_output_columns)
        if is_distinct:
            return remove_duplicates(list_of_tuples)
        return list_of_tuples


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
    if (query[0] == "'"):
        delimiter = "'"
    else:
        delimiter = '"'
    query = query[1:]
    end_quote_index = query.find(delimiter)
    while query[end_quote_index + 1] == delimiter:
        # Remove Escaped Quote
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index = query.find(delimiter, end_quote_index + 1)
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
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] in "(),;*.><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in {"'", '"'}:
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError(
                "Query didn't get shorter. query = {}".format(query))

    return tokens
