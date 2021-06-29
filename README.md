## Snowflake transactions with the NodeJS driver
A very small PoC that shows how transactions can be used with Snowflake's NodeJS driver.

### Setting it up
 - You need to have NodeJS installed locally. 
 - Clone the repository and do a `npm ci` in the root folder.
 - Create a `.env` file in the root folder with the following contents (adjust the values accordingly):

```
SNOWFLAKE_ACCOUNT_NAME=abc1234.us-east-1
SNOWFLAKE_USERNAME=USERNAME
SNOWFLAKE_PASSWORD=PASSWORD
```
 - Run `npm start`.