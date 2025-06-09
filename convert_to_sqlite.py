import os
import re
import sqlite3
import time

def rename_index_column(line):
    """
    Renames the 'index' column to '_index' in SQL statements, ensuring it only applies to column names
    and not to string values or other parts of the SQL.


    Args:
        line (str): The SQL line to process.


    Returns:
        str: The processed SQL line with 'index' renamed to '_index'.
    """
    # Use a regex to match 'index' only when it is a column name
    # Match 'index' when it is surrounded by spaces, commas, parentheses, or the start/end of the line
    line = re.sub(r'(?<!\w)index(?!\w)', '_index', line)
    return line

def convert_postgres_to_sqlite(input_file, output_file):
    """
    Converts a PostgreSQL SQL dump file to an SQLite-compatible SQL file.

    Args:
        input_file (str): Path to the PostgreSQL SQL dump file.
        output_file (str): Path to save the SQLite-compatible SQL file.
    """
    create_table_statements = {}
    alter_table_buffer = ""  # Buffer to store multi-line ALTER TABLE statements

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            # Skip PostgreSQL-specific commands
            if line.startswith("SET ") or line.startswith("SELECT pg_catalog.set_config"):
                continue

            # Remove schema references like "public."
            line = re.sub(r'public\.', '', line)

            # Replace PostgreSQL-specific data types or syntax
            line = line.replace("SERIAL", "INTEGER PRIMARY KEY AUTOINCREMENT")  # Replace SERIAL
            line = line.replace("integer", "INTEGER")  # Ensure INTEGER is used
            line = line.replace("bigint", "INTEGER")  # SQLite uses INTEGER for large integers

            # Rename 'index' to '_index'
            line = rename_index_column(line)

            # Remove PostgreSQL-specific table options
            if "WITH (" in line or "OIDS=" in line:
                continue

            # Capture CREATE TABLE statements
            if line.strip().startswith("CREATE TABLE"):
                table_name = re.search(r'CREATE TABLE (\w+)', line).group(1)
                create_table_statements[table_name] = [line]
                continue

            if line.strip().startswith(");") and create_table_statements:
                # End of CREATE TABLE statement
                table_name = list(create_table_statements.keys())[-1]
                create_table_statements[table_name].append(line)
                outfile.write("".join(create_table_statements[table_name]))
                del create_table_statements[table_name]
                continue

            if create_table_statements:
                # Append lines to the current CREATE TABLE statement
                table_name = list(create_table_statements.keys())[-1]
                create_table_statements[table_name].append(line)
                continue

            # Handle multi-line ALTER TABLE statements
            if line.strip().startswith("ALTER TABLE") or alter_table_buffer:
                if alter_table_buffer:
                    alter_table_buffer += " " + line.strip()  # Ensure space before appending
                else:
                    alter_table_buffer = line.strip()

                if ";" in line:  # End of the ALTER TABLE statement
                    match = re.search(r'ALTER TABLE (\w+) ADD CONSTRAINT \w+ PRIMARY KEY \((.*?)\)', alter_table_buffer)
                    if match:
                        table_name, columns = match.groups()
                        # Search for the corresponding CREATE TABLE statement in the output file
                        with open(output_file, 'r') as temp_file:
                            lines = temp_file.readlines()

                        with open(output_file, 'w') as temp_file:
                            for temp_line in lines:
                                if temp_line.strip().startswith(f"CREATE TABLE {table_name}"):
                                    # Add PRIMARY KEY to the CREATE TABLE statement
                                    temp_file.write(temp_line)
                                    while not temp_line.strip().endswith(");"):
                                        temp_line = next(lines)
                                        if temp_line.strip() == ");":
                                            temp_line = f"    PRIMARY KEY ({columns})\n);\n"
                                        temp_file.write(temp_line)
                                else:
                                    temp_file.write(temp_line)
                    alter_table_buffer = ""  # Clear the buffer
                continue

            # Handle Index Creation
            if line.startswith("CREATE INDEX"):
                # Remove the USING btree part
                line = re.sub(r' USING btree', '', line)
                outfile.write(f"{line}\n")
                continue

            # Write the modified line to the output file
            outfile.write(line)

    print(f"Conversion complete! SQLite-compatible SQL saved to {output_file}")

def import_to_sqlite(db_file, sql_file):
    """
    Imports an SQLite-compatible SQL file into the specified SQLite database with WAL mode enabled.
    Executes CREATE TABLE, INSERT INTO, ADD CONSTRAINT, and CREATE INDEX commands properly, even if they span multiple lines.

    Args:
        db_file (str): Path to the SQLite database file.
        sql_file (str): Path to the SQLite-compatible SQL file.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Enable WAL mode
    cursor.execute("PRAGMA journal_mode=WAL;")

    # Read the SQL file line by line
    with open(sql_file, 'r') as f:
        current_command = ""
        command_count = 0
        start_time = time.time()

        # Keywords to detect the start of a new SQL command
        command_keywords = ("CREATE TABLE", "INSERT INTO", "ADD CONSTRAINT", "CREATE INDEX")

        for line in f:
            line = line.strip()
            if not line or line.startswith('--') or line.startswith('/*'):
                continue  # Skip empty lines and comments

            # If the line starts with a command keyword, process the previous command (if any)
            if any(line.startswith(keyword) for keyword in command_keywords):
                if current_command:  # Execute the accumulated command
                    try:
                        cursor.execute(current_command)
                        command_count += 1
                    except sqlite3.OperationalError as e:
                        print(f"Error executing command: {current_command}\nError: {e}")
                    current_command = ""  # Reset for the next command

            # Accumulate the current line into the command buffer
            current_command += line + " "

            # If the command ends with a semicolon, execute it
            if current_command.endswith(';'):
                try:
                    cursor.execute(current_command)
                    command_count += 1
                except sqlite3.OperationalError as e:
                    print(f"Error executing command: {current_command}\nError: {e}")
                current_command = ""  # Reset for the next command

            # Check if one second has passed for rate reporting
            if time.time() - start_time >= 1:
                print(f"Processed {command_count} commands in the last second.")
                start_time = time.time()  # Reset the timer
                command_count = 0  # Reset the command count

    # Commit transaction
    conn.commit()
    conn.close()
    print(f"Data imported successfully into {db_file}.")

def convert_directory(input_dir, output_dir, db_file):
    """
    Converts all PostgreSQL SQL dump files in a directory to SQLite-compatible SQL files and imports them into an SQLite database.

    Args:
        input_dir (str): Path to the directory containing PostgreSQL SQL dump files.
        output_dir (str): Path to the directory to save SQLite-compatible SQL files.
        db_file (str): Path to the SQLite database file.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for filename in os.listdir(input_dir):
        if filename.endswith('.sql'):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, filename)
            convert_postgres_to_sqlite(input_file, output_file)
            import_to_sqlite(db_file, output_file)

            # Remove the temporary output file after import
            os.remove(output_file)  # comment this line if you want to keep the output files

# Example usage
input_dir = ""  # Replace with your PostgreSQL dump directory
output_dir = ""  # Replace with your desired SQLite dump directory
db_file = "tmp_database.db"  # Replace with your SQLite database file path
convert_directory(input_dir, output_dir, db_file)
