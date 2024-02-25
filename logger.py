from telethon import TelegramClient, events, sync
from telethon import events
from telethon.tl import types
from telethon.tl.types import Message
from tabulate import tabulate
from telethon.types import Channel
import sqlite3
import os
import time  
from dotenv import load_dotenv

# Define the database file path
DB_FILE = 'messages.db'

load_dotenv()

IGNORED_COMMANDS = {'/search', '/logchat', '/usersearch', '/vacuum', '/cleardb', '/dbinfo', '/dbsize', '/globalsearch', '/usermsgsearch'}

# Your user account credentials
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
session_name = os.getenv('TELEGRAM_SESSION_NAME')
messages_stored_by_log_message_to_database = 0

# Function to establish a database connection
def connect_to_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    return conn, cursor

# Function to close the database connection
def close_database_connection(conn):
    conn.commit()
    conn.close()

# Initialize the Telegram client with your user session
client = TelegramClient(session_name, api_id, api_hash)

my_user_id = None

async def get_my_id():
    global my_user_id
    me = await client.get_me()
    my_user_id = me.id

# Create the tables if they don't exist
def create_tables_if_not_exist():
    conn, cursor = connect_to_database()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Users (
            UserID INTEGER PRIMARY KEY,
            Username TEXT,
            FirstName TEXT,
            LastName TEXT,
            OtherInfo TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Chats (
            ChatID TEXT PRIMARY KEY,
            ChatType TEXT,
            OtherInfo TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Messages (
            MessageID INTEGER PRIMARY KEY,
            UserID INTEGER,
            ChatID TEXT,
            MessageText TEXT,
            Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (UserID) REFERENCES Users(UserID),
            FOREIGN KEY (ChatID) REFERENCES Chats(ChatID)
        )
    ''')

    close_database_connection(conn)

create_tables_if_not_exist()

def global_database_search(criteria=None, page=1, page_size=2):
    conn, cursor = connect_to_database()
    # Calculate the offset based on the page number and page size
    offset = (page -  1) * page_size
    
    # Base query to search for messages based on the criteria
    query = '''
        SELECT Messages.MessageID, Users.UserID, Users.Username, Users.FirstName, Users.LastName, Messages.ChatID, Messages.MessageText, Messages.Timestamp
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    '''
    params = ('%' + criteria + '%',)
    
    # Add pagination
    query += '''
        ORDER BY Messages.Timestamp DESC
        LIMIT ? OFFSET ?
    '''
    params += (page_size, offset)
    
    # Execute the query with the parameters
    cursor.execute(query, params)
    search_results = cursor.fetchall()
    
    # Count the total number of results
    cursor.execute('''
        SELECT COUNT(*)
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    ''', ('%' + criteria + '%',))
    total_results = cursor.fetchone()[0]
    
    close_database_connection(conn)
    return search_results, total_results


def search_database(user_id=None, criteria=None, page=1, page_size=2, chat_id=None):
    conn, cursor = connect_to_database()
    # Calculate the offset based on the page number and page size
    offset = (page -   1) * page_size
    
    # Base query to search for messages based on the criteria
    query = '''
        SELECT Messages.MessageID, Users.UserID, Users.Username, Users.FirstName, Users.LastName, Messages.ChatID, Messages.MessageText, Messages.Timestamp
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    '''
    params = ('%' + criteria + '%',)
    
    # Add chat ID filter if provided
    if chat_id is not None:
        query += '''
            AND Messages.ChatID = ?
        '''
        params += (chat_id,)
    
    # Add user ID filter if provided
    if user_id is not None:
        query += '''
            AND Users.UserID = ?
        '''
        params += (user_id,)
    
    # Add pagination
    query += '''
        ORDER BY Messages.Timestamp DESC
        LIMIT ? OFFSET ?
    '''
    params += (page_size, offset)
    
    # Execute the query with the parameters
    cursor.execute(query, params)
    search_results = cursor.fetchall()
    
    # Count the total number of results
    cursor.execute('''
        SELECT COUNT(*)
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    ''', ('%' + criteria + '%',))

    # Add chat ID filter if provided
    if chat_id is not None:
        cursor.execute('''
            SELECT COUNT(*)
            FROM Messages
            JOIN Users ON Messages.UserID = Users.UserID
            WHERE Messages.MessageText LIKE ?
            AND Messages.MessageText NOT LIKE '/search%'
            AND Messages.ChatID = ?
        ''', ('%' + criteria + '%', chat_id))

    total_results = cursor.fetchone()[0]
    
    close_database_connection(conn)
    return search_results, total_results

def search_database_by_criteria_and_username(username=None, criteria=None, page=1, page_size=2, chat_id=None):
    conn, cursor = connect_to_database()
    # Calculate the offset based on the page number and page size
    offset = (page -   1) * page_size
    
    # Base query to search for messages based on the criteria
    query = '''
        SELECT Messages.MessageID, Users.UserID, Users.Username, Users.FirstName, Users.LastName, Messages.ChatID, Messages.MessageText, Messages.Timestamp
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    '''
    params = ('%' + criteria + '%',)
    
    # Add username filter if provided
    if username is not None:
        query += '''
            AND Users.Username = ?
        '''
        params += (username,)
    
    # Add chat ID filter if provided
    if chat_id is not None:
        query += '''
            AND Messages.ChatID = ?
        '''
        params += (chat_id,)
    
    # Add pagination
    query += '''
        ORDER BY Messages.Timestamp DESC
        LIMIT ? OFFSET ?
    '''
    params += (page_size, offset)
    
    # Execute the query with the parameters
    cursor.execute(query, params)
    search_results = cursor.fetchall()
    
    # Count the total number of results
    cursor.execute('''
        SELECT COUNT(*)
        FROM Messages
        JOIN Users ON Messages.UserID = Users.UserID
        WHERE Messages.MessageText LIKE ?
        AND Messages.MessageText NOT LIKE '/search%'
    ''', ('%' + criteria + '%',))

    # Add username and chat ID filters if provided
    if username is not None:
        cursor.execute('''
            SELECT COUNT(*)
            FROM Messages
            JOIN Users ON Messages.UserID = Users.UserID
            WHERE Messages.MessageText LIKE ?
            AND Messages.MessageText NOT LIKE '/search%'
            AND Users.Username = ?
        ''', ('%' + criteria + '%', username))

    if chat_id is not None:
        cursor.execute('''
            SELECT COUNT(*)
            FROM Messages
            JOIN Users ON Messages.UserID = Users.UserID
            WHERE Messages.MessageText LIKE ?
            AND Messages.MessageText NOT LIKE '/search%'
            AND Messages.ChatID = ?
        ''', ('%' + criteria + '%', chat_id))

    if username is not None and chat_id is not None:
        cursor.execute('''
            SELECT COUNT(*)
            FROM Messages
            JOIN Users ON Messages.UserID = Users.UserID
            WHERE Messages.MessageText LIKE ?
            AND Messages.MessageText NOT LIKE '/search%'
            AND Users.Username = ?
            AND Messages.ChatID = ?
        ''', ('%' + criteria + '%', username, chat_id))

    total_results = cursor.fetchone()[0]
    
    close_database_connection(conn)
    return search_results, total_results

@client.on(events.NewMessage(pattern='/usersearch'))  # Listen for the /usersearch command
async def search_criteria_and_username_command(event):
    if event.sender_id != my_user_id:
        return
    
    # Extract the command parts
    command_parts = event.raw_text.split('/usersearch', 1)[-1].strip().split(';')
    
    # Initialize search query, username, chat ID, and pagination parameters to None
    search_query = None
    username = None
    chat_id = None
    page =   1  # Default to the first page
    page_size = 2 # Default number of results per page
    
    # Check if there are command parts
    if command_parts:
        # The first part is always the user
        username = command_parts[0].strip()
        
        # If there's a second part, it's the search query
        if len(command_parts) >   1:
            search_query = command_parts[1].strip()
        
        # If there's a third part, it's the page
        if len(command_parts) >   2:
            page = int(command_parts[2].strip())
        
        # If there's a fourth part, it's the page_size
        if len(command_parts) >   3:
            page_size = int(command_parts[3].strip())
        
        # If there's a fifth part, it's the chat_id
        if len(command_parts) >   4:
            chat_id = int(command_parts[4].strip())
    
    print(f"Search query: {search_query}, Username: {username}, Chat ID: {chat_id}, Page: {page}, Page Size: {page_size}")
    
    # Perform the search with pagination and optional username and chat ID filters
    search_results, total_results = search_database_by_criteria_and_username(username=username, criteria=search_query, page=page, page_size=page_size, chat_id=chat_id)

    total_pages = (total_results + page_size -  1) // page_size

    # Send the search results as a message
    if search_results:
        message = f"Search Results (Page {page} of {total_pages}, {total_results} results):\n\n"
        for result in search_results:
            chat_id = int(result[1])  # Convert chat_id to an integer
            user_id = int(result[5])  # Convert user_id to an integer
            if (chat_id == user_id):
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nMessage:```{result[6]}```\n\n"
            else:
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nLink: https://t.me/c/{result[5]}/{result[0]}\nMessage:```{result[6]}```\n\n"
        await event.respond(message)
    else:
        await event.respond("No results found. Try a different search term or refine your search criteria.")

@client.on(events.NewMessage(pattern='/search'))  # Listen for the /search command
async def search_command(event):
    if event.sender_id != my_user_id:
        return
    
    # Extract the command parts
    command_parts = event.raw_text.split('/search',   1)[-1].strip().split(';')
    
    # Initialize search query, chat ID, user ID, and pagination parameters to None
    search_query = None
    chat_id = None
    user_id = None
    page =  1  # Default to the first page
    page_size = 2  # Default number of results per page
    
    # Check if there are command parts
    if command_parts:
        # The first part is always the user_id
        user_id = command_parts[0].strip()
        
        # If there's a second part, it's the search_query
        if len(command_parts) >   1:
            search_query = command_parts[1].strip()
        
        # If there's a third part, it's the page
        if len(command_parts) >   2:
            page = int(command_parts[2].strip())
        
        # If there's a fourth part, it's the page_size
        if len(command_parts) >   3:
            page_size = int(command_parts[3].strip())
        
        # If there's a fifth part, it's the chat_id
        if len(command_parts) >   4:
            chat_id = int(command_parts[4].strip())
    
    print(f"Search query: {search_query}, Chat ID: {chat_id}, User ID: {user_id}, Page: {page}, Page Size: {page_size}")
    
    # Perform the search with pagination and optional chat ID and user ID filters
    search_results, total_results = search_database(user_id=user_id, criteria=search_query, page=page, page_size=page_size, chat_id=chat_id)

    total_pages = (total_results + page_size -  1) // page_size

    # Send the search results as a message
    if search_results:
        message = f"Search Results (Page {page} of {total_pages}, {total_results} results):\n\n"
        for result in search_results:
            chat_id = int(result[1])  # Convert chat_id to an integer
            user_id = int(result[5])  # Convert user_id to an integer
            if (chat_id == user_id):
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nMessage:```{result[6]}```\n\n"
            else:
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nLink: https://t.me/c/{result[5]}/{result[0]}\nMessage:```{result[6]}```\n\n"
        await event.respond(message)
    else:
        await event.respond("No results found. Try a different search term or refine your search criteria.")

@client.on(events.NewMessage())  # Listen for new messages
async def log_all_messages(event):
    conn, cursor = connect_to_database()
    sender = await event.get_sender()
    chat = await event.get_chat()
    if isinstance(chat, types.User):
        print("Private Chat, not logging")
        return
    if isinstance(sender, types.User):
        if not event.sender.bot:
            # Check if the sender is a user
            # Ignore messages containing commands
            if event.raw_text.split()[0] in IGNORED_COMMANDS:
                return

            # Check if the message contains any text
            if not event.raw_text.strip():
                print("Message without text: Skipping.")
                return

            # Insert or retrieve user from the 'Users' table
            user_id = sender.id
            username = sender.username if sender.username else None
            first_name = sender.first_name if sender.first_name else None
            last_name = sender.last_name if sender.last_name else None
            cursor.execute('''
                INSERT OR IGNORE INTO Users (UserID, Username, FirstName, LastName)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name))

            # Insert or retrieve chat from the 'Chats' table
            chat_id = str(chat.id)
            chat_type = chat.stringify()
            cursor.execute('''
                INSERT OR IGNORE INTO Chats (ChatID, ChatType)
                VALUES (?, ?)
            ''', (chat_id, chat_type))

            # Insert the message into the 'Messages' table
            message_id = event.message.id
            message_text = event.raw_text
            cursor.execute('''
                INSERT INTO Messages (MessageID, UserID, ChatID, MessageText)
                VALUES (?, ?, ?, ?)
            ''', (message_id, user_id, chat_id, message_text))
           
        close_database_connection(conn)

# Define the command to trigger logging of all current messages from the chat where the command is being run
@client.on(events.NewMessage(pattern='/logchat'))  # Listen for the /log_current_chat_messages command
async def log_current_chat_messages_command(event):
    if event.sender_id != my_user_id:
        return
    # Correctly await the get_chat() method to get the chat object
    chat = await event.get_chat()

    await event.respond("Started!")

    fetch_start_time = time.time()
    
    # Use client.get_messages to fetch messages from the chat
    try:
        messages = await client.get_messages(chat, limit=300000)  # Adjust the limit as needed
    except Exception as e:
        print("FUCK")
        print(e)

    # Measure the time after fetching messages
    fetch_end_time = time.time()
    fetch_elapsed_time_ms = (fetch_end_time - fetch_start_time) *  1000  # Convert to milliseconds
    fetch_elapsed_time_min = fetch_elapsed_time_ms / (1000 *  60)  # Convert to minutes
    
    # Log each message
    for message in messages:
        await log_message_to_database(message)

    # Measure the time after writing messages to the database
    write_end_time = time.time()
    write_elapsed_time_ms = (write_end_time - fetch_end_time) *  1000  # Convert to milliseconds
    write_elapsed_time_sec = write_elapsed_time_ms /  1000  # Convert to seconds
    
    await event.respond(f"Logging complete!\nFetching messages took {fetch_elapsed_time_ms:.2f} ms [{fetch_elapsed_time_min:.2f} minutes]\nWriting to the database took {write_elapsed_time_ms:.2f} ms [{write_elapsed_time_sec:.2f} seconds]")

async def log_message_to_database(message):
    # Check if the message is an instance of the Message class
    if not isinstance(message, Message):
        print("Error: The provided object is not a Message instance.")
        return
    
    global messages_stored_by_log_message_to_database

    conn, cursor = connect_to_database()
    sender = await message.get_sender()
    chat = await message.get_chat()
    
    if isinstance(sender, types.User):
        if not message.sender.bot:
            # Check if the message contains any text
            if not message.text.strip():
                return

            user_id = sender.id
            username = sender.username if sender.username else None
            first_name = sender.first_name if sender.first_name else None
            last_name = sender.last_name if sender.last_name else None
            cursor.execute('''
                INSERT OR IGNORE INTO Users (UserID, Username, FirstName, LastName)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name))

            # Insert or retrieve chat from the 'Chats' table
            chat_id = str(chat.id)
            chat_type = chat.stringify()
            cursor.execute('''
                INSERT OR IGNORE INTO Chats (ChatID, ChatType)
                VALUES (?, ?)
            ''', (chat_id, chat_type))

            # Insert the message into the 'Messages' table
            message_id = message.id
            message_text = message.text
            cursor.execute('''
                INSERT OR IGNORE INTO Messages (MessageID, UserID, ChatID, MessageText)
                VALUES (?, ?, ?, ?)
            ''', (message_id, user_id, chat_id, message_text))
        
        messages_stored_by_log_message_to_database = messages_stored_by_log_message_to_database + 1
        print(f"Stored a new message! ({messages_stored_by_log_message_to_database})")

        close_database_connection(conn)

@client.on(events.NewMessage(pattern='/cleardb'))  # Listen for the /clear_db command
async def clear_db_command(event):
    if event.sender_id != my_user_id:
        return
    
    conn, cursor = connect_to_database()
    
    # Drop all tables
    cursor.execute('''
        DROP TABLE IF EXISTS Messages;
    ''')
    cursor.execute('''
        DROP TABLE IF EXISTS Users;
    ''')
    cursor.execute('''
        DROP TABLE IF EXISTS Chats;
    ''')
    
    # Recreate the database and tables
    create_tables_if_not_exist()
    
    close_database_connection(conn)
    
    await event.respond("Database contents have been cleared.")

@client.on(events.NewMessage(pattern='/vacuum'))  # Listen for the /vacuum_db command
async def vacuum_db_command(event):
    if event.sender_id != my_user_id:
        return
    
    conn, cursor = connect_to_database()
    cursor.execute("VACUUM;")
    conn.commit()
    close_database_connection(conn)
    
    await event.respond("Database has been vacuumed.")

@client.on(events.NewMessage(pattern='/dbinfo'))  # Listen for the /db_info command
async def db_info_command(event):
    if event.sender_id != my_user_id:
        return
    
    conn, cursor = connect_to_database()
    
    # Query for the number of messages
    cursor.execute('SELECT COUNT(*) FROM Messages;')
    message_count = cursor.fetchone()[0]
    
    # Query for the number of users
    cursor.execute('SELECT COUNT(*) FROM Users;')
    user_count = cursor.fetchone()[0]
    
    # Query for the number of separate chats (groups and supergroups)
    cursor.execute('SELECT COUNT(*) FROM Chats WHERE ChatType LIKE "%group%" OR ChatType LIKE "%supergroup%";')
    chat_count = cursor.fetchone()[0]
    
    # Query for the total number of letters in messages
    cursor.execute('SELECT SUM(LENGTH(MessageText)) FROM Messages;')
    total_letters = cursor.fetchone()[0]
    
    # Query for the distribution of messages across different chats (groups and supergroups)
    cursor.execute('''
        SELECT ChatID, COUNT(*) AS MessageCount
        FROM Messages
        WHERE ChatID IN (SELECT ChatID FROM Chats WHERE ChatType LIKE "%group%" OR ChatType LIKE "%supergroup%")
        GROUP BY ChatID
        ORDER BY MessageCount DESC;
    ''')
    chat_message_distribution = cursor.fetchall()
    
    # Query for the average message length
    cursor.execute('SELECT AVG(LENGTH(MessageText)) FROM Messages;')
    average_message_length = round(cursor.fetchone()[0]) 
    
    close_database_connection(conn)
    
    # Format the information into a single message
    info_message = f"""Database Information:
- Messages Stored: {message_count}
- Users: {user_count}
- Separate Chats: {chat_count}
- Total Letters in Messages: {total_letters}
- Average Message Length: {average_message_length} characters
- Top Chats by Message Count:"""
    # Use tabulate to format the "Top Chats by Message Count" section
    table_headers = ["Chat ID", "Messages"]
    table_data = [[chat_id, message_count] for chat_id, message_count in chat_message_distribution]
    table = tabulate(table_data, headers=table_headers, tablefmt="grid")
    info_message += f"```{table}```"
    
    # Send the single message with all information
    await event.respond(info_message)

@client.on(events.NewMessage(pattern='/usermsgcount'))  # Listen for the /user_message_count command
async def user_message_count_command(event):
    if event.sender_id != my_user_id:
        return
    
    # Extract the user ID from the command
    command_parts = event.raw_text.split('/usermsgcount',  1)[-1].strip().split(';')
    user_id = command_parts[0].strip() if command_parts else None
    
    if not user_id:
        await event.respond("Please provide a user ID after the command.")
        return
    
    conn, cursor = connect_to_database()
    
    # Query to count the number of messages sent by the user
    cursor.execute('''
        SELECT COUNT(*)
        FROM Messages
        WHERE UserID = ?
    ''', (user_id,))
    message_count = cursor.fetchone()[0]
    
    close_database_connection(conn)
    
    await event.respond(f"User {user_id} has sent {message_count} messages.")

@client.on(events.NewMessage(pattern='/dbsize'))  # Listen for the /dbsize command
async def database_size_command(event):
    if event.sender_id != my_user_id:
        return
    
    # Get the size of the database file
    db_size_bytes = os.path.getsize(DB_FILE)
    
    # Convert bytes to a more readable format (e.g., KB, MB)
    if db_size_bytes <   1024:
        db_size_str = f"{db_size_bytes} bytes"
    elif db_size_bytes <   1024 *   1024:
        db_size_str = f"{db_size_bytes /   1024:.2f} KB"
    else:
        db_size_str = f"{db_size_bytes / (1024 *   1024):.2f} MB"
    
    await event.respond(f"The current database size is {db_size_str}.")

@client.on(events.NewMessage(pattern='/globalsearch'))  # Listen for the /globalsearch command
async def global_search_command(event):
    if event.sender_id != my_user_id:
        return
    
    # Extract the search criteria and page number from the command
    command_parts = event.raw_text.split('/globalsearch',   1)[-1].strip().split(';')
    search_query = command_parts[0].strip() if command_parts else None
    page_number = int(command_parts[1].strip()) if len(command_parts) >  1 else  1  # Default to page  1 if not specified
    
    if not search_query:
        await event.respond("Please provide a search query after the command.")
        return

    # Define the page size
    page_size =  2  # You can adjust this number based on your preference
    
    # Perform the global search with the criteria and page number
    search_results, total_results = global_database_search(criteria=search_query, page=page_number, page_size=page_size)
    
    total_pages = (total_results + page_size -  1) // page_size

    # Send the search results as a message
    if search_results:
        message = f"Global Search Results (Page  1 of {total_pages}, {total_results} results):\n\n"
        for result in search_results:
            chat_id = int(result[1])  # Convert chat_id to an integer
            user_id = int(result[5])  # Convert user_id to an integer
            if (chat_id == user_id):
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nMessage:```{result[6]}```"
            else:
                message += f"User: {result[2]} ({result[1]}) [{result[3]} {result[4]}]\nMessage ID: {result[0]} [Chat ID: {result[5]}]\nTimestamp: {result[7]}\nLink: https://t.me/c/{result[5]}/{result[0]}\nMessage:```{result[6]}```"
        await event.respond(message)
    else:
        await event.respond("No results found. Try a different search term.")  

async def main():
    # Start the client and run the event loop
    await client.start()
    await get_my_id()
    await client.run_until_disconnected()   

client.loop.run_until_complete(main()) 
