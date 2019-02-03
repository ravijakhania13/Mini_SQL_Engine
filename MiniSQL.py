from prettytable import PrettyTable
from heapq import merge
import csv
import sqlparse
import sys
import os
import re
import numpy as np

Path = "./"

def Get_All_Tables_Headers(All_Tables,All_Tables_Headers):
    Headers = []
    Table_Name = ""
    Previous_Line = ""
    with open(Path+'metadata.txt') as f:
        My_Lines = f.read().splitlines()
        for Line in My_Lines:
            if(Previous_Line == "<begin_table>"):
                Table_Name = Line
            elif(Line == "<end_table>"):
                All_Tables_Headers[Table_Name] = Headers
                All_Tables[Table_Name] = list(csv.reader(open(Path+Table_Name+".csv"))) #[list(map(int,rec)) for rec in csv.reader(open(Path+Table_Name+".csv"), delimiter=',')]
                for i in range(0,len(All_Tables[Table_Name])):
                    for j in range(0,len(All_Tables[Table_Name][i])):
                        if("“" in All_Tables[Table_Name][i][j] or '"' in All_Tables[Table_Name][i][j]):
                            All_Tables[Table_Name][i][j] = All_Tables[Table_Name][i][j][1:-1]
                        All_Tables[Table_Name][i][j] = int(All_Tables[Table_Name][i][j])
                Headers = []
            elif(Line != "<begin_table>"):
                Headers.append(Line)
            Previous_Line = Line
    return All_Tables,All_Tables_Headers

def Get_Tables(Table_List,All_Tables):
    List_of_tables = {}
    Table_List = Table_List.split(',')
    for Table_Name in Table_List:
        List_of_tables[Table_Name] = All_Tables[Table_Name]
    return List_of_tables

def Get_Headers(Table_List,All_Tables_Headers):
    List_of_table_Headers = {}
    Table_List = Table_List.split(',')
    for Table_Name in Table_List:
        List_of_table_Headers[Table_Name] = All_Tables_Headers[Table_Name]
    return List_of_table_Headers

def Get_Distinct_Headers(Headers):
    Distinct_Headers = []
    for tables in Headers.values():
        for column in tables:
            if column not in Distinct_Headers:
                Distinct_Headers.append(column)
    return Distinct_Headers

def show_table(table,headers):
    print (",".join(map(str,headers)))
    for row in table:
        print (",".join(map(str,row)))

def Projection(Tables,Table_Headers,Common_Headers):

    Final_Table = []
    Final_Headers = []
    for Table in Tables.keys():
        for Header_Name in Table_Headers[Table]:
            Final_Headers.append(Table+"."+Header_Name)
            Index_Table = list(Tables.keys()).index(Table)
            Index_Header = Table_Headers[Table].index(Header_Name)
            if (len(Final_Table) <= Index_Table):
                Final_Table.append([])
                for row in Tables[Table]:
                    Final_Table[Index_Table].append([row[Index_Header]])
            else:
                i = 0
                for row in Tables[Table]:
                    Final_Table[Index_Table][i].append(row[Index_Header])
                    i += 1
    List_Length = []
    
    for Tbl in Final_Table:
        List_Length.append(len(Tbl))
    List_Length.append(1)
    
    for i in range(len(List_Length)-2,-1,-1):
        List_Length[i] *= List_Length[i+1]
    List_Length = List_Length[1:]
    
    Latest_Table = []
    for Row in Final_Table[0]:
        for i in range(0,List_Length[0]):
            Latest_Table.append(Row)

    for table_index in range(1,len(Final_Table)):
        k = 0
        while k < len(Latest_Table):
            for row in Final_Table[table_index]:
                for j in range(0,List_Length[table_index]):
                    Temp = Latest_Table[k] + row
                    Latest_Table[k] = Temp
                    k += 1

    return Latest_Table,Final_Headers

def Differentiate(Conditions):
    Where_Cols = []
    Operators = []
    Values = []
    Connector = ""

    if ("and" in Conditions.lower()):
        Connector = "and"
    elif("or" in Conditions.lower()):
        Connector = "or"

    Conditions = re.split("AND|OR|and|and", Conditions)
    for Condition in Conditions:
        Condition = Condition.replace(" ","")
        if("<=" in Condition):
            Index = Condition.index("<")
            Operators.append("<=")
            Where_Cols.append(Condition[:Index])
            Values.append(Condition[Index+2:])
        elif(">=" in Condition):
            Index = Condition.index(">")
            Operators.append(">=")
            Where_Cols.append(Condition[:Index])
            Values.append(Condition[Index+2:])
        elif(">" in Condition):
            Index = Condition.index(">")
            Operators.append(">")
            Where_Cols.append(Condition[:Index])
            Values.append(Condition[Index+1:])
        elif("<" in Condition):
            Index = Condition.index("<")
            Operators.append("<")
            Where_Cols.append(Condition[:Index])
            Values.append(Condition[Index+1:])
        elif("=" in Condition):
            Index = Condition.index("=")
            Operators.append("==")
            Where_Cols.append(Condition[:Index])
            Values.append(Condition[Index+1:])
        else:
            error = 1
            print ("Syntax Error in Where Clause!")
            sys.exit(1)

    return Where_Cols,Operators,Values,Connector

def Headers_Without_Table(Headers_After_Projection):
    Headers_After_Projection_Without_Table_Name = []
    for header in Headers_After_Projection:
        Index = header.find(".")
        Headers_After_Projection_Without_Table_Name.append(header[Index+1:])
    return Headers_After_Projection_Without_Table_Name

def WhereCondition(Table_After_Projection,Headers_After_Projection,Headers_After_Projection_Without_Table_Name,Where_Cols,Operators,Values,Connector):
    if(len(Where_Cols) == 1):
        if (Where_Cols[0] in Headers_After_Projection and Values[0] in Headers_After_Projection):
            Index_1 = Headers_After_Projection.index(Where_Cols[0])
            Index_2 = Headers_After_Projection.index(Values[0])
            
            List_Temp = [v for v in Table_After_Projection if (eval(str(v[Index_1]) + " " + Operators[0] + " " + str(v[Index_2])))]#v[Index_1] == v[Index_2]]
            
            Table_After_Projection = List_Temp
            [r.pop(Index_2) for r in Table_After_Projection]
            Headers_After_Projection.pop(Index_2)
            Headers_After_Projection_Without_Table_Name.pop(Index_2)

        elif (Where_Cols[0] in Headers_After_Projection or Where_Cols[0] in Headers_After_Projection_Without_Table_Name):
            Index = 0
            List_Temp = []
            if (Where_Cols[0] in Headers_After_Projection):
                Index = Headers_After_Projection.index(Where_Cols[0])
            else:
                Index = Headers_After_Projection_Without_Table_Name.index(Where_Cols[0])
            List_Temp = [v for v in Table_After_Projection if (eval(str(v[Index]) + " " + Operators[0] + " " + str(Values[0])))]
            Table_After_Projection = List_Temp
        else:
            error = 1
            print ("Syntax Error. Wrong Query!")
            sys.exit(1)
    else:
        if ((Where_Cols[0] in Headers_After_Projection and Values[0] in Headers_After_Projection) and (Where_Cols[1] in Headers_After_Projection and Values[1] in Headers_After_Projection)):

            Index_1 = Headers_After_Projection.index(Where_Cols[0])
            Index_2 = Headers_After_Projection.index(Where_Cols[0])
            Index_3 = Headers_After_Projection.index(Values[0])
            Index_4 = Headers_After_Projection.index(Values[1])
            List_Temp = [v for v in Table_After_Projection if (eval(str(v[Index_1]) + " " + Operators[0] + " " + str(v[Index_3])+ " " + Connector.lower() + " " + str(v[Index_2]) + " " + Operators[1] + " " + str(v[Index_4])))] #v[Index_1] == v[Index_3] and v[Index_2] == v[Index_4]]
            Table_After_Projection = List_Temp

            Index_3 = Headers_After_Projection.index(Values[0])

            [r.pop(Index_3) for r in Table_After_Projection]
            Headers_After_Projection.pop(Index_3)
            Headers_After_Projection_Without_Table_Name.pop(Index_3)

            Index_4 = Headers_After_Projection.index(Values[1])

            [r.pop(Index_4) for r in Table_After_Projection]
            Headers_After_Projection.pop(Index_4)
            Headers_After_Projection_Without_Table_Name.pop(Index_4)
        elif ((Where_Cols[0] in Headers_After_Projection or Where_Cols[0] in Headers_After_Projection_Without_Table_Name) and (Where_Cols[1] in Headers_After_Projection or Where_Cols[1] in Headers_After_Projection_Without_Table_Name)):
            Index_1 = 0
            Index_2 = 0
            List_Temp = []
            if (Where_Cols[0] in Headers_After_Projection):
                Index_1 = Headers_After_Projection.index(Where_Cols[0])
            else:
                Index_1 = Headers_After_Projection_Without_Table_Name.index(Where_Cols[0])

            if (Where_Cols[1] in Headers_After_Projection):
                Index_2 = Headers_After_Projection.index(Where_Cols[1])
            else:
                Index_2 = Headers_After_Projection_Without_Table_Name.index(Where_Cols[1])
            
            List_Temp = [v for v in Table_After_Projection if (eval(str(v[Index_1]) + " " + Operators[0] + " " + str(Values[0])+ " " + Connector.lower() + " " + str(v[Index_2]) + " " + Operators[1] + " " + str(Values[1])))]
            Table_After_Projection = List_Temp
        else:
            error = 1
            print ("Syntax Error. Wrong Query!")
            sys.exit(1)
    return Table_After_Projection,Headers_After_Projection,Headers_After_Projection_Without_Table_Name

def CheckAggregate(Columns):
    Final_Columns = []
    Final_Aggregate = []
    for Column in Columns.split(','):
        Column = Column.replace(" ","")
        if (Column.lower().startswith("max(")):
            Final_Aggregate.append("max")
            Final_Columns.append(Column[4:-1])
        elif (Column.lower().startswith("min(")):
            Final_Aggregate.append("min")
            Final_Columns.append(Column[4:-1])
        elif (Column.lower().startswith("sum(")):
            Final_Aggregate.append("sum")
            Final_Columns.append(Column[4:-1])
        elif (Column.lower().startswith("avg(")):
            Final_Aggregate.append("avg")
            Final_Columns.append(Column[4:-1])
        elif("(" not in Column.lower()):
            Final_Columns.append(Column)
        else:
            error = 1
            print ("Syntax Error. Wrong Query!")
            sys.exit(1)
    return Final_Columns, Final_Aggregate

def Select_Execution(Table,Headers,Headers_Without_Table_Name,Columns):
    Order = [-1] * len(Headers)
    Curr_Index = 0
    Column_Index = []

    for Column in Columns:
        if (Column in Headers):
            Order[Headers.index(Column)] = Curr_Index
            Column_Index.append(Curr_Index)
            Curr_Index += 1
        elif (Column in Headers_Without_Table_Name):
            if(Headers_Without_Table_Name.count(Column) > 1):
                error = 1
                print ("Ambiguous Query!")
                sys.exit(1)
            Order[Headers_Without_Table_Name.index(Column)] = Curr_Index
            Column_Index.append(Curr_Index)
            Curr_Index += 1
        else:
            error = 1
            print ("Column name not found!")
            sys.exit(1)

    for i in range(0,len(Order)):
        if (Order[i] == -1):
            Order[i] = Curr_Index
            Curr_Index += 1

    Headers = [item[0] for item in (sorted(zip(Headers,Order), key=lambda x: x[1]))]
    Headers_Without_Table_Name = [item[0] for item in (sorted(zip(Headers_Without_Table_Name,Order), key=lambda x: x[1]))]

    for i in range(0,len(Table)):
        Table[i] = [item[0] for item in (sorted(zip(Table[i],Order), key=lambda x: x[1]))]

    for k in range(0,len(Table)):
        Table[k] = [i for j, i in enumerate(Table[k]) if j in Column_Index]

    Headers = [i for j, i in enumerate(Headers) if j in Column_Index]
    Headers_Without_Table_Name = [i for j, i in enumerate(Headers_Without_Table_Name) if j in Column_Index]

    return Table,Headers,Headers_Without_Table_Name

def Aggregate_Execute(Table,Headers,Aggregate):
    New_Table = []
    New_Headers = []
    for i in range (0,len(Aggregate)):
        if (Aggregate[i].lower() == "sum"):
            if (len(Table)):
                New_Table.append(sum([row[i] for row in Table]))
            New_Headers.append("sum("+Headers[i]+")")
        elif(Aggregate[i].lower() == "min"):
            if (len(Table)):
                New_Table.append(min([row[i] for row in Table]))
            New_Headers.append("min("+Headers[i]+")")
        elif(Aggregate[i].lower() == "max"):
            if (len(Table)):
                New_Table.append(max([row[i] for row in Table]))
            New_Headers.append("max("+Headers[i]+")")
        elif(Aggregate[i].lower() == "avg"):
            if (len(Table)):
                New_Table.append(sum([row[i] for row in Table])/len([row[i] for row in Table]))
            New_Headers.append("avg("+Headers[i]+")")
        else:
            error = 1
            print ("Syntax Error. Wrong Query!")
            sys.exit(1)

    return [New_Table],New_Headers            

def Distinct_Execute(Table):
    return [list(t) for t in set(tuple(element) for element in Table)]


def main():
    Query = sys.argv[1]
    
    if Query[-1] == ";":
        Query = Query[:-1]
    parsed = sqlparse.parse(Query)

    Query_Stmt = []
    for i in parsed[0]:
        if(not str(i).startswith(" ") or  not str(i).endswith(" ")):
            Query_Stmt.append(str(i))

    Columns = ""
    Tables_Name = ""
    Conditions = ""
    Aggregate = ""
    Distinct = False
    Where = False
    IsAggregate = False

    if ("distinct" in Query_Stmt[1].lower()):
        Distinct = True
        Query_Stmt.pop(1)

    i = 0
    while i < len(Query_Stmt):
        if Query_Stmt[i].lower() == "select":
            i = i + 1
            Columns = Query_Stmt[i]
            i = i + 1
        elif Query_Stmt[i].lower() == "from":
            i = i + 1
            Tables_Name = Query_Stmt[i]
            i = i + 1
        elif Query_Stmt[i].lower() == "  " or Query_Stmt[i].lower() == " ":
            i = i + 1
        elif Query_Stmt[i].lower().startswith("where "):
            Where = True
            Conditions = Query_Stmt[i][6:]
            i = i + 1
        else:
            error = 1
            print ("Syntax Error. Wrong Query!")
            sys.exit(1)

    Columns = Columns.replace(" ","")
    Tables_Name = Tables_Name.replace(" ","")

    All_Tables,All_Tables_Headers = Get_All_Tables_Headers({},{})
    
    for table in Tables_Name.split(','):
        if table not in All_Tables.keys():
            print ("Error Occurred. Table " + table + " Not Found!")
            sys.exit(1)

    Tables = Get_Tables(Tables_Name,All_Tables)

    Headers = Get_Headers(Tables_Name,All_Tables_Headers)

    Dictinct_Headers = Get_Distinct_Headers(Headers)

    Table,Headers = Projection(Tables,Headers,Dictinct_Headers)

    Headers_Without_Table_Name = Headers_Without_Table(Headers)

    if (Where):
        Where_Cols,Operators,Values,Connector = Differentiate(Conditions)
        for table_name in Where_Cols:
            if (table_name not in Headers and table_name not in Headers_Without_Table_Name):
                print ("Error Occurred. Column name " + table_name + " used in where clause is not Found in Any Table!")
                sys.exit(1)
        Table,Headers,Headers_Without_Table_Name = WhereCondition(Table,Headers,Headers_Without_Table_Name,Where_Cols,Operators,Values,Connector)

    Columns,Aggregate = CheckAggregate(Columns)

    if(len(Aggregate) > 0 and len(Aggregate)!=len(Columns)):
        error = 1
        print ("Syntax Error. Wrong Query!")
        sys.exit(1)
    elif(len(Aggregate) > 0):
        IsAggregate = True

    if (Columns[0] != "*"):
        Table,Headers,Headers_Without_Table_Name = Select_Execution(Table,Headers,Headers_Without_Table_Name,Columns)

    if (IsAggregate):
        Table,Headers = Aggregate_Execute(Table,Headers,Aggregate)

    if(Distinct):
        Table = Distinct_Execute(Table)
        
    show_table(Table,Headers)

if __name__ == '__main__':
    main()