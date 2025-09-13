#!/usr/bin/env python3
"""
Database Seed Script for ALX_prodev
This script sets up the MySQL database with user_data table and populates it with sample data.
No external imports - pure Python implementation.
"""

import sys
import os


def generate_uuid():
    """Generate a simple UUID-like string without importing uuid module."""
    import random
    import time
    
    # Create a simple UUID-like format: 8-4-4-4-12
    chars = '0123456789abcdef'
    random.seed(int(time.time() * 1000000) % 2**32)
    
    uuid_parts = [
        ''.join(random.choice(chars) for _ in range(8)),
        ''.join(random.choice(chars) for _ in range(4)),
        ''.join(random.choice(chars) for _ in range(4)),
        ''.join(random.choice(chars) for _ in range(4)),
        ''.join(random.choice(chars) for _ in range(12))
    ]
    
    return '-'.join(uuid_parts)


def connect_db():
    """
    Connects to the MySQL database server.
    
    Returns:
        Connection object if successful, None otherwise
    """
    try:
        # Try to import mysql.connector
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host='localhost',
            user='root',  # Change as needed
            password='',  # Add your MySQL password here
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        
        if connection.is_connected():
            print("✅ Successfully connected to MySQL server")
            return connection
            
    except ImportError:
        print("❌ Error: mysql-connector-python not installed.")
        print("Please install it using: pip install mysql-connector-python")
        return None
    except Exception as e:
        print(f"❌ Error connecting to MySQL server: {e}")
        return None


def create_database(connection):
    """
    Creates the database ALX_prodev if it does not exist.
    
    Args:
        connection: MySQL connection object
        
    Returns:
        bool: True if database created or already exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        
        # Check if database was created
        cursor.execute("SHOW DATABASES LIKE 'ALX_prodev'")
        result = cursor.fetchone()
        
        if result:
            print("✅ Database ALX_prodev created successfully or already exists")
            cursor.close()
            return True
        else:
            print("❌ Failed to create database ALX_prodev")
            cursor.close()
            return False
            
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False


def connect_to_prodev():
    """
    Connects to the ALX_prodev database in MySQL.
    
    Returns:
        Connection object if successful, None otherwise
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host='localhost',
            user='root',  # Change as needed
            password='',  # Add your MySQL password here
            database='ALX_prodev',
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        
        if connection.is_connected():
            print("✅ Successfully connected to ALX_prodev database")
            return connection
            
    except ImportError:
        print("❌ Error: mysql-connector-python not installed.")
        return None
    except Exception as e:
        print(f"❌ Error connecting to ALX_prodev database: {e}")
        return None


def create_table(connection):
    """
    Creates a table user_data if it does not exist with the required fields.
    
    Args:
        connection: MySQL connection object to ALX_prodev database
        
    Returns:
        bool: True if table created successfully, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        # SQL query to create user_data table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(3,0) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_user_id (user_id),
            INDEX idx_email (email),
            INDEX idx_age (age),
            INDEX idx_name (name)
        )
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        
        print("✅ Table user_data created successfully or already exists")
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False


def insert_data(connection, data):
    """
    Inserts data into the database if it does not exist.
    
    Args:
        connection: MySQL connection object to ALX_prodev database
        data: List of dictionaries containing user data
        
    Returns:
        bool: True if data inserted successfully, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM user_data")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"ℹ️ Database already contains {existing_count} records. Skipping data insertion.")
            cursor.close()
            return True
        
        # Prepare insert query with ON DUPLICATE KEY UPDATE to avoid duplicates
        insert_query = """
        INSERT INTO user_data (user_id, name, email, age) 
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            name = VALUES(name),
            email = VALUES(email),
            age = VALUES(age),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Process data in batches for better performance
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            # Prepare batch data for insertion
            batch_values = []
            for record in batch:
                # Generate UUID if not provided
                if 'user_id' not in record or not record['user_id']:
                    record['user_id'] = generate_uuid()
                
                # Validate and convert age to decimal
                try:
                    age = int(float(record['age']))
                except (ValueError, TypeError):
                    print(f"⚠️ Invalid age value for {record.get('name', 'Unknown')}: {record.get('age', 'N/A')}")
                    age = 0
                
                batch_values.append((
                    record['user_id'],
                    record['name'],
                    record['email'],
                    age
                ))
            
            # Execute batch insert
            cursor.executemany(insert_query, batch_values)
            connection.commit()
            
            total_inserted += len(batch)
            print(f"📊 Inserted batch: {len(batch)} records (Total: {total_inserted})")
        
        print(f"✅ Successfully inserted {total_inserted} records into user_data table")
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        connection.rollback()
        return False


def parse_csv_line(line):
    """
    Simple CSV line parser without importing csv module.
    
    Args:
        line: String line from CSV file
        
    Returns:
        List of values
    """
    # Handle quoted fields and commas
    values = []
    current_value = ""
    in_quotes = False
    
    i = 0
    while i < len(line):
        char = line[i]
        
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            values.append(current_value.strip())
            current_value = ""
        else:
            current_value += char
        i += 1
    
    # Add the last value
    values.append(current_value.strip())
    
    # Remove quotes from values
    cleaned_values = []
    for value in values:
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        cleaned_values.append(value)
    
    return cleaned_values


def load_csv_data(csv_file_path):
    """
    Load data from CSV file without importing csv module.
    
    Args:
        csv_file_path: Path to the CSV file
        
    Returns:
        List of dictionaries containing user data
    """
    data = []
    
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        return data
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            
            if not lines:
                print("❌ CSV file is empty")
                return data
            
            # Parse header
            header_line = lines[0].strip()
            headers = parse_csv_line(header_line)
            
            # Parse data rows
            for row_num, line in enumerate(lines[1:], 2):
                line = line.strip()
                if not line:
                    continue
                
                values = parse_csv_line(line)
                
                # Ensure we have the right number of values
                if len(values) != len(headers):
                    print(f"⚠️ Skipping row {row_num}: Column count mismatch")
                    continue
                
                # Create dictionary from headers and values
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header.lower().strip()] = values[i] if i < len(values) else ''
                
                # Basic validation
                name = row_dict.get('name', '').strip()
                email = row_dict.get('email', '').strip()
                
                if not name or not email:
                    print(f"⚠️ Skipping row {row_num}: Missing required fields")
                    continue
                
                # Clean data
                clean_row = {
                    'user_id': row_dict.get('user_id', '').strip() or generate_uuid(),
                    'name': name,
                    'email': email,
                    'age': row_dict.get('age', '0').strip()
                }
                
                data.append(clean_row)
        
        print(f"📖 Successfully loaded {len(data)} records from {csv_file_path}")
        return data
        
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return []


def create_sample_csv(csv_file_path="user_data.csv"):
    """
    Creates the provided CSV file with real user data.
    
    Args:
        csv_file_path: Path where to create the CSV file
    """
    if os.path.exists(csv_file_path):
        print(f"CSV file {csv_file_path} already exists")
        return
    
    # The actual CSV data from the document provided
    csv_content = '''"name","email","age"
"Johnnie Mayer","Ross.Reynolds21@hotmail.com","35"
"Myrtle Waters","Edmund_Funk@gmail.com","99"
"Flora Rodriguez I","Willie.Bogisich@gmail.com","84"
"Dr. Cecilia Konopelski-Lakin","Felicia75@gmail.com","87"
"Chelsea Boyle-Stoltenberg","Regina.Emard97@yahoo.com","83"
"Seth Mraz","Cecilia_Blanda89@gmail.com","24"
"Thelma Kris-Schinner","Johnnie.Jast93@hotmail.com","6"
"Thomas Hane","Dominic24@yahoo.com","93"
"Della Hickle","Leon_Rohan@hotmail.com","35"
"Kristi Durgan","Maria_Schmeler9@hotmail.com","70"
"Brad Sawayn","Tyler.Dach57@gmail.com","112"
"Isabel Crist Jr.","Cecilia_Braun54@yahoo.com","63"
"Allen Roob","Sandra.Heidenreich4@yahoo.com","40"
"Robin Wilkinson","Brent_Wilkinson2@hotmail.com","62"
"Martin Flatley","Gabriel23@hotmail.com","13"
"Delia Walker IV","Leticia_Schinner@yahoo.com","78"
"Blanca Durgan","Christina27@gmail.com","1"
"Ellen Hudson","Matthew.Medhurst69@gmail.com","111"
"Bobby Bayer","Erick_Brekke36@gmail.com","110"
"Karen Pfannerstill","Amber.Steuber-Greenfelder@gmail.com","49"
"Vanessa Kihn-Durgan","Lorena.Schuppe@hotmail.com","49"
"Grace Sporer","George38@yahoo.com","50"
"Krista Herzog-Paucek","Shawn.Tremblay@hotmail.com","109"
"Doyle Schaden","Clarence.Berge@hotmail.com","30"
"Beth Crooks","Sean.Bradtke99@yahoo.com","36"
"Doyle Botsford","Wilfred.Dickinson@hotmail.com","70"
"Santos Skiles","Joey22@gmail.com","17"
"Ms. Gina Kuhic","Debbie83@hotmail.com","78"
"Garry Pfeffer","Lora_Heathcote91@yahoo.com","105"
"Annie Rogahn","June.Kuhn24@hotmail.com","92"
"Hubert Gerlach","Alice99@hotmail.com","3"
"Clark Willms","Leo25@gmail.com","45"
"Natalie Lesch PhD","Marilyn76@yahoo.com","76"
"Pauline Cremin","Geraldine.Langworth27@hotmail.com","50"
"Nellie Labadie","Clay75@hotmail.com","27"
"Felipe Barrows","Clint37@yahoo.com","47"
"Derrick Mitchell Jr.","Mark_Fritsch67@hotmail.com","52"
"James Boehm","Terry9@hotmail.com","54"
"Darrin Fritsch","Leland.Mills@yahoo.com","50"
"Blanca Osinski","Annette56@yahoo.com","96"
"Inez Walker","Fannie_Wolff-Schinner@gmail.com","19"
"Deanna Kunze","Colleen.Hayes@gmail.com","109"
"Angela Emmerich","Martin12@yahoo.com","1"
"Sidney Kertzmann","Angelo_Krajcik@yahoo.com","10"
"Miguel Stokes Sr.","Sheila86@hotmail.com","82"
"Cedric McLaughlin","Al.Towne@yahoo.com","21"
"Kendra Hauck","Jordan1@gmail.com","58"
"Spencer Larson","Rickey65@gmail.com","110"
"Claudia Collins","Timothy12@gmail.com","3"
"Ernest Frami","Elijah_Mante32@gmail.com","108"
"Fred Bailey","Shaun_Pagac@yahoo.com","23"
"Lillie Feeney MD","Lester32@hotmail.com","29"
"Salvador Collier","Alfredo.Bogisich68@gmail.com","50"
"Kathleen Prosacco","Ervin.Nitzsche16@hotmail.com","5"
"Arlene Bednar","Rickey7@yahoo.com","24"
"Gayle Towne","Cora.Nitzsche@yahoo.com","108"
"Alison Carroll","William.Wisoky@hotmail.com","5"
"Simon Legros","Lindsey6@gmail.com","61"
"Lucas Larkin","Chelsea_Kohler@hotmail.com","20"
"Dr. Jerome Jaskolski","Russell_VonRueden93@yahoo.com","107"
"Delores Friesen","Shaun.Jacobson21@yahoo.com","76"
"Owen Walker","Sophie.Will35@yahoo.com","110"
"Danny Rippin","Darryl_Mann@hotmail.com","22"
"Mona Maggio-Schimmel","Albert6@hotmail.com","34"
"Dr. Myra Jacobson PhD","Rudy_Shields59@hotmail.com","23"
"Eunice Crona","Leland.Harris@hotmail.com","82"
"Essie Ortiz","Johnnie.Rutherford-Beatty5@yahoo.com","47"
"Lucia Ullrich","Dallas71@yahoo.com","81"
"Michael Wintheiser","Brendan_Hilll@hotmail.com","75"
"Tanya Lemke","Jessie_OConner38@hotmail.com","117"
"Marian Kassulke","Yvonne25@hotmail.com","104"
"Aubrey Senger","Darrell.Feeney0@yahoo.com","62"
"Mrs. Alexis Runolfsson","Krista.Crooks@hotmail.com","84"
"Van Klocko","Derrick.Collier53@yahoo.com","95"
"Veronica Dicki","Horace.Gulgowski56@hotmail.com","3"
"Paulette Abshire","Velma_Flatley26@gmail.com","25"
"Jill Senger MD","Keith64@hotmail.com","81"
"Betsy Stracke","Sidney.Wolff38@yahoo.com","11"
"Dr. Ronald Murray","Billie.Runolfsson@hotmail.com","15"
"Mr. Marc Zieme","Wendy.Kessler-Adams91@yahoo.com","53"
"Danielle Von","Delbert.Robel@gmail.com","34"
"Mack Moore","Sherry45@yahoo.com","96"
"Mrs. Sabrina Cassin","Marshall_Daugherty20@yahoo.com","104"
"Cameron Bailey","Anita_Pagac@yahoo.com","9"
"Lewis Nolan Sr.","Geraldine.Crooks18@gmail.com","7"
"Sally Howell","Mamie.Parisian@hotmail.com","97"
"Dr. Rosie King","Dale19@yahoo.com","51"
"Dwight Raynor","Yvonne_Barrows13@yahoo.com","65"
"Debbie Parker","George_Bahringer@hotmail.com","33"
"Carolyn Denesik","Jean.Jacobi@yahoo.com","5"
"Lula Pfeffer","Inez.Brown25@gmail.com","22"
"Danielle Block","Lauren_Crooks74@hotmail.com","109"
"Alexis Runolfsdottir DDS","Dan81@hotmail.com","41"
"Jeremy Weber","Enrique20@hotmail.com","75"
"Claudia Kreiger","Krystal.Ziemann27@hotmail.com","90"
"Thomas McLaughlin","Angelo_Gleichner@yahoo.com","118"
"Bradley Bogisich","Marlon.Oberbrunner@yahoo.com","17"
"Elsie Schmitt","Julio55@yahoo.com","2"
"Fernando Wiegand","Paul.Lindgren59@hotmail.com","20"
"Mr. Clay Kirlin-Mann","Lucia_Powlowski87@yahoo.com","91"
"Grace Vandervort","Maria_Gulgowski@yahoo.com","84"
"Monica Abbott","Elsie.Kuphal76@hotmail.com","88"
"Nancy Mueller","Jimmy_Wolf@yahoo.com","42"
"Andrea Bernier-Abbott","Forrest43@hotmail.com","16"
"Mr. Carlos Abbott","Edna_Spinka@hotmail.com","50"
"Carl Tromp-Mraz","Lucas.Will60@gmail.com","3"
"Irma Kutch","Marcus65@gmail.com","77"
"Jake Ankunding","Marion_Hessel58@gmail.com","75"
"Clint Lebsack","Jennifer.Schimmel@yahoo.com","22"
"Miss Ana Pouros","Kelvin68@yahoo.com","80"
"Sheldon Daniel","Stuart.Larkin96@hotmail.com","113"
"Travis Crooks","Adrienne.OConner71@yahoo.com","22"
"Clifton Dickinson","Melvin_Bergstrom@yahoo.com","97"
"Saul Waelchi","Kristi_Connelly70@yahoo.com","19"
"Julie Mueller","Roger.Jenkins25@hotmail.com","40"
"Estelle Krajcik","Cristina_Little72@hotmail.com","63"
"Andy Streich","Marcella.Stanton32@hotmail.com","21"
"Jonathon Volkman","Irving.Hackett@yahoo.com","92"
"Patti O'Conner-Baumbach","Irma.Cruickshank@hotmail.com","6"
"Dwayne Gottlieb","Brandi.Kulas@yahoo.com","32"
"Kellie Howe","Toni.Legros@hotmail.com","87"
"Mr. Virgil Mraz","Johnathan33@gmail.com","23"
"Mr. Justin Bosco","Kurt.Kshlerin@gmail.com","94"
"Stanley Becker","Charles42@yahoo.com","33"
"Roxanne Koss","Robin_Zulauf@yahoo.com","102"
"Ann Corkery","Lana.Kuvalis@hotmail.com","79"
"Danny Witting","Patsy16@yahoo.com","3"
"Byron Kihn","Sherri.Dietrich@gmail.com","17"
"Roy Sporer","Russell_Jast64@gmail.com","34"
"Lois Grimes","Perry_Bradtke@gmail.com","52"
"Sheldon Hyatt","Patricia_Terry@gmail.com","3"
"Cecilia Brown","Blanche.Bergstrom7@yahoo.com","29"
"Nathan Dach-O'Kon","Morris.Runolfsdottir49@yahoo.com","100"
"Ellis Prohaska","Ben68@yahoo.com","58"
"Jeffery Reilly","Adrian.Mitchell1@yahoo.com","116"
"Rodolfo Emmerich","Betty.Boyer@hotmail.com","98"
"Marcus Reichel","Guillermo_Spencer@hotmail.com","72"
"Johnathan Hills V","Kurt59@yahoo.com","97"
"Malcolm Daugherty","Candice13@hotmail.com","5"
"Hilda Beahan","Jeanette_Breitenberg55@hotmail.com","40"
"Billie Rippin","Sadie17@yahoo.com","78"
"Alison Lemke","Dana33@yahoo.com","117"
"Jamie Nitzsche","Byron.Bogan50@hotmail.com","28"
"Jason Tillman","Lydia.Reilly@yahoo.com","54"
"Darryl Orn","Wilbur.Schmitt@hotmail.com","21"
"Bill Wisoky","Alexander_Pacocha@hotmail.com","78"
"Mr. Jesus Kunde V","Rudy37@hotmail.com","113"
"Earnest Mosciski","Freda32@yahoo.com","85"
"Kristie Krajcik","Amanda5@hotmail.com","113"
"Misty Erdman-Schulist","Kelvin96@hotmail.com","94"
"Tanya Champlin","Garrett.Turner@yahoo.com","101"
"Regina Kuhlman II","Colleen18@hotmail.com","91"
"Beverly Koch-Runte","Christian.Herman72@yahoo.com","91"
"Connie Ernser","Willard55@hotmail.com","104"
"Angelina Bailey DDS","Terry.Stokes@gmail.com","54"
"Rhonda Altenwerth","Randolph.Hermann@yahoo.com","3"
"Kelly Bosco-Swift","Janet29@gmail.com","3"
"Ms. Alyssa Donnelly","Pat56@hotmail.com","52"
"Delbert Sauer Jr.","Michelle_Renner58@hotmail.com","85"
"Frank Aufderhar","Nettie14@gmail.com","76"
"Mr. Kenny Wiegand","Ken_Kuhn@hotmail.com","92"
"Mr. Eduardo Fisher","Felicia_Streich@yahoo.com","57"
"Archie Kreiger","Dennis_Donnelly@gmail.com","32"
"Tanya Swaniawski","Patty14@hotmail.com","97"
"Edwin Gislason","Maureen.Smith77@hotmail.com","100"
"Victor Raynor","Rebecca75@gmail.com","27"
"Claudia Ziemann PhD","Janet_Krajcik@yahoo.com","49"
"Sylvester Jerde","Raul.Zulauf18@gmail.com","30"
"Tabitha McClure IV","Traci8@yahoo.com","100"
"Jaime Zboncak","Dana18@gmail.com","39"
"Martin Howell","Teri.Wiza-Cummings85@gmail.com","24"
"Eula Denesik","Orlando85@gmail.com","103"
"Edmond Jerde","Eloise.Vandervort55@hotmail.com","111"
"Dianna Gutmann","Nick_Hackett@yahoo.com","56"
"Irvin Block DDS","Noel.Lockman85@yahoo.com","59"
"Boyd Feil","Loren.Veum32@yahoo.com","27"
"Geoffrey Hills","Robin10@yahoo.com","56"
"Mr. Hector Mayer","Desiree.Heller@gmail.com","61"
"Frederick Bednar","Brandy.Boehm@hotmail.com","31"
"Lillian Douglas","Sheila_Harvey@gmail.com","24"
"Wanda Mohr","Kathryn_Brown10@hotmail.com","100"
"Dixie Bins","Cecil35@gmail.com","113"
"Marian Lind-Flatley","Roger.Hilll34@gmail.com","65"
"Miss Sandra Johnston","Pedro.Hilll26@hotmail.com","81"
"Lela Crooks","Robin71@hotmail.com","21"
"Darryl Mraz","Marta74@hotmail.com","86"
"Shane White","Penny.Waelchi@yahoo.com","67"
"Tara Hintz MD","Robin_Kuphal18@gmail.com","91"
"Dr. Jesse Blanda-Weissnat","Mark.Wiza@yahoo.com","55"
"Josefina Sanford","Michelle.Prohaska@hotmail.com","50"
"Rosemarie Gutkowski","Lindsay.McClure-Dibbert20@gmail.com","29"
"Lonnie Davis","Kathleen.Spinka25@yahoo.com","48"
"Charlotte Kub","Simon76@yahoo.com","110"
"Dr. Laurence Thiel","Laurie.Lemke20@yahoo.com","22"
"Dan Willms","Nichole77@gmail.com","50"
"Shawn Funk","Suzanne.Koelpin17@gmail.com","11"
"Sherry Morar","Mario27@hotmail.com","6"
"Sandra Yundt","Gladys.Gutkowski@yahoo.com","49"
"Miss Olivia Connelly","Bradford.Runte@hotmail.com","50"
"Vincent Brown","Cornelius.Fahey@hotmail.com","30"
"Dr. Rosa Howell","Tara.Schumm43@hotmail.com","71"
"Amy Kilback","Laurie.Green67@gmail.com","54"
"Sherman Herzog","Alexis33@yahoo.com","103"
"Shelia Wilkinson","Dominic78@hotmail.com","47"
"Evelyn Dare IV","Emanuel_Purdy18@yahoo.com","5"
"Elias Grady","Melvin.Ritchie85@yahoo.com","81"
"Randy Aufderhar","Marcos96@hotmail.com","61"
"Damon Moore","Kristen.Rosenbaum95@gmail.com","9"
"Merle Funk","Geoffrey.Harber60@gmail.com","106"
"Marcia Buckridge","Salvatore46@yahoo.com","48"
"Christine Bartoletti","Gordon76@hotmail.com","37"
"Ms. Christina Herzog","Rita.Abernathy@hotmail.com","44"
"Milton Herzog I","Joel_Funk@hotmail.com","117"
"Aaron Will","Zachary_Harris51@hotmail.com","85"
"Elvira McClure","Jasmine10@yahoo.com","45"
"Chester Glover","Jacob59@gmail.com","117"
"Mr. Daniel Hilpert","Bonnie_Adams35@gmail.com","12"
"Patsy Harber","Marvin.Swift79@gmail.com","30"
"Nichole Friesen","Douglas_Krajcik@yahoo.com","49"
"Lewis Gerhold","Jermaine74@yahoo.com","3"
"Mr. Rick Ankunding","Rene28@yahoo.com","86"
"Freddie Boyer","Gene85@yahoo.com","82"
"Ira Armstrong","Jean_Ernser@yahoo.com","43"
"Miss Anita West","Beth_Franey2@hotmail.com","8"
"Dexter Jacobson","Eva_Rice@gmail.com","109"
"Beth Terry","Elsie10@hotmail.com","105"
"Mr. Mark Cruickshank-Veum","Dean74@gmail.com","111"
"Dewey Dickens","Clara_Wisozk77@yahoo.com","15"
"Sean Bartell","Nora61@hotmail.com","53"
"Ms. Gloria Franecki","Charlie_Bruen88@yahoo.com","3"
"Erik Flatley","Marguerite_Ziemann@hotmail.com","47"
"Ian Prosacco Jr.","Laurie_Ankunding30@hotmail.com","105"
"Zachary Hane","Christy82@gmail.com","34"
"Dan Wilkinson","Emmett83@gmail.com","48"
"Josh Littel","Paulette.Bogisich@yahoo.com","50"
"Tara Reynolds","Clifford.Schultz@yahoo.com","36"
"Mr. Lonnie Reichert","Fred_Jakubowski@hotmail.com","45"
"Miss Jessie Cole","Elmer73@gmail.com","16"
"John Ondricka","Billy.Koepp@yahoo.com","30"
"Bryant Orn","Nathaniel.Goyette@yahoo.com","46"
"Ronnie Bechtelar","Sandra19@yahoo.com","22"
"Darrin Schulist","Edna53@hotmail.com","109"
"Jeremy Jaskolski","Walter_Haley@hotmail.com","26"
"Pablo Dietrich","Rhonda_Kemmer@hotmail.com","46"
"Lisa Emard","William.Conroy3@gmail.com","100"
"Jo Braun","Jan_Beatty@gmail.com","50"
"Tiffany Larson","Victor.Wilkinson-Ullrich@hotmail.com","97"
"Miss Angelina Smitham","Scott.Price22@hotmail.com","6"
"Darnell Hahn","Penny.Brakus@gmail.com","34"
"Doreen Lindgren","Emilio.Reynolds33@yahoo.com","56"
"Keith Kuvalis","Krista.Weber85@yahoo.com","69"
"Gordon Armstrong","Angelina_Schoen11@gmail.com","114"
"Dr. Ivan Corwin","Olivia_Stracke@gmail.com","34"
"Alma Bechtelar","Shelly_Balistreri22@hotmail.com","102"
"Chelsea Konopelski","Pamela.Balistreri@yahoo.com","108"
"Mrs. June Skiles","Maurice.Kris51@yahoo.com","102"
"Gary Barton","Frederick_Kerluke@gmail.com","24"
"Ms. Francis Harber-Franecki","Karen_Ratke@gmail.com","100"
"Clay Roberts","Marlene.Frami35@hotmail.com","95"
"Dwayne Lebsack","Amber.Carroll45@yahoo.com","118"
"Joshua Wunsch-Harvey","Guadalupe.Fisher43@yahoo.com","43"
"Roman Huels","Harriet.Cronin10@yahoo.com","70"
"Josefina Pfannerstill","Molly.Ortiz42@gmail.com","68"
"Gerard Spinka","Raul_Conroy74@hotmail.com","109"
"Clifford Kris","Carol89@hotmail.com","49"
"Bernadette Gulgowski","Mitchell.Jacobi72@yahoo.com","50"
"Mrs. Angel O'Reilly","Ella.Heaney@hotmail.com","115"
"Catherine Homenick","Stella_McKenzie44@gmail.com","88"
"Lorene Kassulke","Nancy_Leannon@hotmail.com","55"
"Harriet Murazik","Sophia66@gmail.com","119"
"Elizabeth Monahan","Levi.Powlowski7@hotmail.com","75"
"Mario Kassulke","Joshua_Borer@gmail.com","30"
"Helen Bernhard","Robert98@yahoo.com","65"
"Ms. Sue Harris","Calvin.Abshire23@yahoo.com","80"
"Erik Friesen","Mary_Spencer60@gmail.com","25"
"Vera Funk","Stewart_Jast@yahoo.com","117"
"Everett Ryan","Sherri_Macejkovic61@gmail.com","66"
"Cora Zieme-Schinner","Clifford66@hotmail.com","82"
"Melanie Hessel","Kristine6@gmail.com","93"
"Miss Michele Okuneva","Jennie_Purdy@yahoo.com","87"
"Travis Rice V","Elmer_Terry34@gmail.com","61"
"Wilson Schmitt","Jermaine.Spinka75@yahoo.com","55"
"Rita Bernhard","Kara.Rutherford89@hotmail.com","67"
"Lionel Graham Jr.","Marie_Nitzsche3@hotmail.com","99"
"Larry Heathcote","Francisco15@hotmail.com","23"
"Benny Feil","Opal.Smitham25@gmail.com","53"
"Dr. Gerard Krajcik","Clayton_Mitchell67@gmail.com","60"
"Jessica Leffler","Terri64@gmail.com","17"
"Mrs. Meredith Rowe","Beverly_Cruickshank@hotmail.com","88"
"Brian Hettinger-Bode","Laurie34@hotmail.com","54"
"Herman Green","Lauren_Halvorson23@hotmail.com","61"
"Carla Nader","Cesar_Zieme90@yahoo.com","103"
"Dan Altenwerth Jr.","Molly59@gmail.com","67"
"Christy Bode","Cameron5@yahoo.com","9"
"Darrel Denesik","Rolando.Borer48@gmail.com","5"
"Molly Emard","Deanna88@hotmail.com","111"
"Forrest Nader","Marcella.Blanda61@yahoo.com","75"
"Mr. Calvin Rippin II","Ann.Kessler@yahoo.com","10"
"June Walker","Joann40@hotmail.com","8"
"Ms. Evelyn Conn","Eunice_Johns@hotmail.com","73"
"Sara Ledner","Alexandra62@yahoo.com","18"
"Randall Koepp","Marcos71@yahoo.com","28"
"Stewart Deckow","Brenda_Harris25@hotmail.com","58"
"Alejandro Goyette MD","Jesse_Schmeler@yahoo.com","31"
"Ms. Virginia Bernier","Brian.Walker71@gmail.com","52"
"Mr. Jaime Watsica","Lula54@gmail.com","53"
"Ms. Tricia Lockman","Curtis.Pfannerstill@yahoo.com","94"
"Darla Hartmann","Cesar.Luettgen@gmail.com","85"
"Gregory Mayer","Rolando32@yahoo.com","98"
"Duane Daugherty","Andy_Ziemann68@yahoo.com","110"
"Anne Waters","Debra39@gmail.com","14"
"Dr. Lee Hintz-Krajcik V","Cary84@gmail.com","97"
"Alex Kreiger","Noel.Lakin93@gmail.com","31"
"Erma Kuvalis","Andy69@gmail.com","86"
"Tracy Howell","Abraham.Kreiger@gmail.com","34"
"Danielle Thiel","Emily_Lebsack11@hotmail.com","47"
"Mr. Cameron Hyatt","Willie70@hotmail.com","109"
"Stacey Wisoky","Mindy_Dietrich48@


def stream_users(connection):
    """
    Generator function that streams rows from the user_data table one by one.
    
    Args:
        connection: MySQL connection object to ALX_prodev database
        
    Yields:
        dict: User record as dictionary
    """
    try:
        cursor = connection.cursor()
        
        # Execute query to get all users
        cursor.execute("SELECT user_id, name, email, age, created_at, updated_at FROM user_data ORDER BY created_at")
        
        # Yield rows one by one
        while True:
            row = cursor.fetchone()
            if row is None:
                break
                
            # Convert row to dictionary
            user_dict = {
                'user_id': row[0],
                'name': row[1],
                'email': row[2],
                'age': int(row[3]),
                'created_at': row[4],
                'updated_at': row[5]
            }
            
            yield user_dict
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Error streaming data: {e}")


def test_generator(connection):
    """
    Test the generator by streaming and displaying some users.
    
    Args:
        connection: MySQL connection object to ALX_prodev database
    """
    print("\n🔄 Testing data streaming generator...")
    
    try:
        count = 0
        for user in stream_users(connection):
            if count < 5:  # Show first 5 users
                print(f"User {count + 1}: {user['name']} ({user['email']}) - Age: {user['age']}")
                count += 1
            else:
                break
        
        # Count total users using generator
        total_count = sum(1 for _ in stream_users(connection))
        print(f"\n📊 Total users streamed: {total_count}")
        
    except Exception as e:
        print(f"❌ Error testing generator: {e}")


def main():
    """
    Main function to orchestrate the database setup and data seeding process.
    """
    print("🚀 Starting database seeding process...")
    
    # Step 1: Connect to MySQL server
    print("\n📡 Step 1: Connecting to MySQL server...")
    server_connection = connect_db()
    if not server_connection:
        print("❌ Failed to connect to MySQL server. Exiting...")
        sys.exit(1)
    
    # Step 2: Create database
    print("\n🗃️ Step 2: Creating ALX_prodev database...")
    if not create_database(server_connection):
        print("❌ Failed to create database. Exiting...")
        server_connection.close()
        sys.exit(1)
    
    # Close server connection
    server_connection.close()
    
    # Step 3: Connect to ALX_prodev database
    print("\n🔗 Step 3: Connecting to ALX_prodev database...")
    db_connection = connect_to_prodev()
    if not db_connection:
        print("❌ Failed to connect to ALX_prodev database. Exiting...")
        sys.exit(1)
    
    # Step 4: Create table
    print("\n📋 Step 4: Creating user_data table...")
    if not create_table(db_connection):
        print("❌ Failed to create table. Exiting...")
        db_connection.close()
        sys.exit(1)
    
    # Step 5: Load and insert data
    print("\n📊 Step 5: Loading and inserting data...")
    csv_file_path = "user_data.csv"
    
    # Create sample CSV if it doesn't exist
    create_sample_csv(csv_file_path)
    
    # Load data from CSV
    data = load_csv_data(csv_file_path)
    if not data:
        print("❌ No data to insert. Exiting...")
        db_connection.close()
        sys.exit(1)
    
    # Insert data into database
    if not insert_data(db_connection, data):
        print("❌ Failed to insert data. Exiting...")
        db_connection.close()
        sys.exit(1)
    
    # Step 6: Verify data insertion
    print("\n✅ Step 6: Verifying data insertion...")
    try:
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_data")
        count = cursor.fetchone()[0]
        print(f"📊 Total records in user_data table: {count}")
        
        # Show sample data
        cursor.execute("SELECT user_id, name, email, age FROM user_data LIMIT 5")
        sample_records = cursor.fetchall()
        print("\n📋 Sample records:")
        print("User ID\t\t\t\t\tName\t\tEmail\t\t\tAge")
        print("-" * 80)
        for record in sample_records:
            print(f"{record[0][:8]}...\t{record[1]:<15}\t{record[2]:<25}\t{record[3]}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
    
    # Step 7: Test the generator
    test_generator(db_connection)
    
    # Close database connection
    db_connection.close()
    print("\n🎉 Database seeding completed successfully!")


if __name__ == "__main__":
    main()
