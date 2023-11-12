# TdWebSocketSvc

Flask app that exposes an api for connecting to a websocket for real-time market data from TD Ameritrade.

[Main Repo](https://github.com/faquino08/FinanceDb/blob/main/README.md)

# Docker Reference

The following is a description of each env var key and value:

**Key Name:** POSTGRES_DB \
**Description:** a string containing the name of the postgres database for data insertion. \
**Values:** <span style="color:#6C8EEF">\<postgres database name string></span>

**Key Name:** POSTGRES_USER \
**Description:**  a string containing the username the postgres server to use for authentication. \
**Values:** <span style="color:#6C8EEF">\<postgres username string></span>

**Key Name:** POSTGRES_PASSWORD \
**Description:** a string containing the password the postgres user specified. \
**Values:** <span style="color:#6C8EEF">\<postgres password string></span>

**Key Name:** POSTGRES_LOCATION \
**Description:** a string containing the hostname for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres hostname string></span>

**Key Name:** POSTGRES_PORT \
**Description:** a string containing the port for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres port string></span>

**Key Name:** DEBUG_BOOL \
**Description:** a string determining whether logging should include debug level messages. \
**Values:** <span style="color:#6C8EEF">True|False</span>

**Key Name:** BOOTSTRAP_SERVER \
**Description:** a string containing the hostname and port of one of the kafka nodes in the cluster. \
**Values:** <span style="color:#6C8EEF">host:port</span>

**Key Name:** SCHEMA_REG_ADDRESS \
**Description:** a string containing the hostname and port of schema registry for the kafka cluster. \
**Values:** <span style="color:#6C8EEF">host:port</span>

**Key Name:** INGRESS_TOPIC \
**Description:** a string containing the name of the kafka topic to ingest raw data from the websocket. \
**Values:** <span style="color:#6C8EEF">host:port</span>

**Key Name:** APP_NAME \
**Description:** a string assigning an app name to be used when logging workflow runs in the database. \
**Values:** <span style="color:#6C8EEF">\<app name string></span>\
**Default:** **TdStream**

# Api Reference

[comment]: <> (First Command)
### <span style="color:#6C8EEF">**POST**</span> /run
Command to connect to the web socket and receive streaming data.

[comment]: <> (Second Command)
### <span style="color:#6C8EEF">**GET**</span> /stop
Command to close the connection to the websocket and stop the process.

[comment]: <> (Third Command)
### <span style="color:#6C8EEF">**POST**</span> /two_fa?futuresRun=<span style="color:#a29bfe">**:int**</span>&futuresRun=<span style="color:#a29bfe">**:boolean**</span>
Command to close the connection to the websocket and stop the process.

#### **Arguments:**
- **code** - security code receied by sms so it can complete the login flow and retreive the authentication token to connect to the streaming socket.
- **futuresRun** - whether or not to insert futures streaming data into Kafka. *Default:* **False**
