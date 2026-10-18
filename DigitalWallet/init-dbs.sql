\getenv account_db_password ACCOUNT_DB_PASSWORD
\getenv transfer_db_password TRANSFER_DB_PASSWORD
\getenv wallet_db_password WALLET_DB_PASSWORD
\getenv payment_db_password PAYMENT_DB_PASSWORD
\getenv notification_db_password NOTIFICATION_DB_PASSWORD
\getenv query_db_password QUERY_DB_PASSWORD

CREATE ROLE account_user LOGIN PASSWORD :'account_db_password';
CREATE ROLE transfer_user LOGIN PASSWORD :'transfer_db_password';
CREATE ROLE wallet_user LOGIN PASSWORD :'wallet_db_password';
CREATE ROLE payment_user LOGIN PASSWORD :'payment_db_password';
CREATE ROLE notification_user LOGIN PASSWORD :'notification_db_password';
CREATE ROLE query_user LOGIN PASSWORD :'query_db_password';

CREATE DATABASE accountdb OWNER account_user;
CREATE DATABASE transferdb OWNER transfer_user;
CREATE DATABASE walletdb OWNER wallet_user;
CREATE DATABASE paymentdb OWNER payment_user;
CREATE DATABASE notificationdb OWNER notification_user;
CREATE DATABASE querydb OWNER query_user;

REVOKE CONNECT ON DATABASE accountdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE transferdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE walletdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE paymentdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE notificationdb FROM PUBLIC;
REVOKE CONNECT ON DATABASE querydb FROM PUBLIC;
