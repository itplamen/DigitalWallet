\getenv account_db_password ACCOUNT_DB_PASSWORD
\getenv transfer_db_password TRANSFER_DB_PASSWORD
\getenv wallet_db_password WALLET_DB_PASSWORD

CREATE ROLE account_user LOGIN PASSWORD :'account_db_password';
CREATE ROLE transfer_user LOGIN PASSWORD :'transfer_db_password';
CREATE ROLE wallet_user LOGIN PASSWORD :'wallet_db_password';

CREATE DATABASE accountdb OWNER account_user;
CREATE DATABASE transferdb OWNER transfer_user;
CREATE DATABASE walletdb OWNER wallet_user;

REVOKE CONNECT ON DATABASE accountdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE transferdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE walletdb FROM PUBLIC;
