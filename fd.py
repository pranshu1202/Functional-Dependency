# Importing python -- postgres connector pyscopg2
import psycopg2

# Establishing connection with Database Postgres
connection = psycopg2.connect(database="institute", user = "postgres", password = "postgres", host = "127.0.0.1", port = "5432")

# Successfully connected to Database Postgres
print ("Opened database successfully")

# Creation of cursor for the connection
cursor = connection.cursor()

# Dropping table "student" if exists previously
cursor.execute("drop table if exists student;")

# Creating table "student"
cursor.execute("Create table student(roll_no int,name varchar(20),standard int,marks int,phone int);")

# Enter number of FD's exist in Database
num = int(input("Enter number of FD's : "))

# For each FD create trigger and table temp
for i in range (0,num):
    
    # Dropping all temporary tables if exists previously
    cursor.execute(f"drop table if exists temp{i+1};")

    # Input FD's  Determinant -> Determiner
    determinant = input(f'Enter {i+1} determinant : ')
    determiner = input(f'Enter {i+1} determiner : ')

    # Creating temp table for each FD 
    cursor.execute(f"Create table temp{i+1} AS select {determinant},{determiner} from student;")

    # Create Index on (Determinant,Determiner)
    cursor.execute(f"CREATE  INDEX idx{i+1} ON temp{i+1} ({determinant},{determiner});")

    # Partition Determinant to handle composite keys separated by commas (,)
    det = determinant.split(',')
    det_new = " "
    for s in det :
        det_new = det_new + 'new.'+s+','
    det_new = det_new[:-1]

    # Partition Determiner to handle composite keys separated by commas (,)
    dtr = determiner.split(',')
    dtr_new = " "
    for s in dtr :
        dtr_new = dtr_new + 'new.'+s+','
    dtr_new = dtr_new[:-1]

    # Dropping all Triggers if exists
    cursor.execute(f"drop trigger if exists checkfd{i+1} on student;")
    cursor.execute(f"drop trigger if exists checkfd1{i+1} on student;")
    cursor.execute(f"drop trigger if exists checkfd2{i+1} on student;")


    # Insert operation may violate FD's
    cursor.execute(f"""create or replace function CheckDependency{i+1}() returns trigger as
    '
    declare
    x int;
    y int;
    begin
        insert into temp{i+1} values({det_new},{dtr_new});
        select count(distinct ({determinant},{determiner}))from temp{i+1} into x;
        select count(distinct ({determinant})) from temp{i+1} into y;
        if x!=y then
            raise exception '' Violates Functional Dependency (({determinant} -> {determiner})) on Insertion '';
        end if;
        return new;
    end;
    'language'plpgsql';""")

    # Insertion Trigger
    cursor.execute(f"""create trigger checkfd{i+1}
    before insert on student
    for each row execute procedure CheckDependency{i+1}()""")


    # Update Operation may violate FD's
    cursor.execute(f""" create or replace function CheckUpdateDependency{i+1}() returns trigger as
    '
    declare
    x int;
    y int;
    begin
        
        drop table if exists temp{i+1};
        Create table temp{i+1} AS select {determinant},{determiner} from student;
        select count(distinct ({determinant},{determiner})) from temp{i+1} into x;
        select count(distinct ({determinant})) from temp{i+1} into y;
        if x!=y then
            raise exception '' Violates Functional Dependency (({determinant} -> {determiner})) on Updation '';
        end if;
        return new;
    end;
    'language'plpgsql'; """)

    # Updation Trigger
    cursor.execute(f""" create trigger checkfd1{i+1}
    after update on student
    for each row execute procedure CheckUpdateDependency{i+1}()""")


    # Delete does not afects FD violation , but temp table for FD need to be altered
    cursor.execute(f""" create or replace function CheckDeleteDependency{i+1}() returns trigger as
    '
    declare
   
    begin
        drop table if exists temp{i+1};
        Create table temp{i+1} AS select {determinant},{determiner} from student;
        return new;
    end;
    'language'plpgsql'; """)

    # Deletion Trigger
    cursor.execute(f""" create trigger checkfd2{i+1}
    after delete on student
    for each row execute procedure CheckDeleteDependency{i+1}()""")

# Connection commit , changes written to database
connection.commit()

# Connection Close
connection.close()


