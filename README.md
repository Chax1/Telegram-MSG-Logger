## Telethon Telegram Client with SQLite Database

This Python script utilizes Telethon to interact with the Telegram API and SQLite database for message logging and searching functionalities.

### Features:

- **Logging Messages**: Logs messages from chats to an SQLite database.
- **Searching Messages**: Provides commands to search for messages based on various criteria such as text content, user ID, or chat ID.
- **Database Management**: Includes commands to clear the database, vacuum the database for optimization, and retrieve database information like size and statistics.
- **Global Search**: Allows for searching messages across all chats based on a specified search query.

### Setup:

1. **Environment Variables**:
   - Ensure you have set up environment variables for `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and `TELEGRAM_SESSION_NAME`.
   
2. **Dependencies**:
   - Install the required dependencies using `pip install -r requirements.txt`.

3. **Run**:
   - Execute the script to start the Telethon client and run the event loop.

### Commands:

- `/logchat`: Log all current messages from the chat where the command is invoked.
- `/search`: Search for messages based on criteria like user ID, chat ID, or text content.
- `/usersearch`: Search for messages based on username and additional criteria like chat ID or text content.
- `/cleardb`: Clear the contents of the database.
- `/vacuum`: Optimize the database by vacuuming.
- `/dbinfo`: Retrieve information about the database such as message count, user count, etc.
- `/usermsgcount`: Count the number of messages sent by a specific user.
- `/dbsize`: Get the current size of the SQLite database.
- `/globalsearch`: Perform a global search across all chats based on a specified query.

### Note:
- Ensure the script has the necessary permissions to access and modify the SQLite database file.
- Customize and extend the functionalities as per your requirements.

**Disclaimer:** Use responsibly and ensure compliance with Telegram API usage policies and guidelines.
